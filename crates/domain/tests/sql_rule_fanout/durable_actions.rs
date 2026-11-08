use super::*;
use domain::{
    DurableProposal, MessageRecord, MessageRecordFormat, OverlayProperty, QueueCounters,
    SqlRuleAction,
};

#[path = "review_fixes.rs"]
mod review_fixes;

fn action<S: StateStore>(
    t: &Topology,
    machine: &StateMachine<S>,
    sub: &str,
    name: &str,
    filter: RuleFilter,
    source: &str,
    at: u64,
) -> TestResult {
    let command = Command::new(
        t.namespace.clone(),
        t.subscription(sub),
        Timestamp::from_millis(at),
        CommandKind::CreateRuleWithAction {
            name: RuleName::new(name)?,
            filter,
            action: SqlRuleAction::new(source),
        },
    );
    let bytes = DurableProposal::unbound(command.clone()).encode()?;
    assert_eq!(&bytes[4..8], &10u32.to_be_bytes());
    let restored = DurableProposal::decode(&bytes)?;
    assert_eq!(restored.command(), &command);
    machine.apply(restored.command())?;
    Ok(())
}

fn setup<S: StateStore>(t: &Topology, machine: &StateMachine<S>) -> TestResult {
    create_topic(t, machine, &t.topic)?;
    t.filtered(
        machine,
        "copies",
        2,
        &[("plain-a", RuleFilter::True), ("plain-b", RuleFilter::True)],
    )?;
    action(
        t,
        machine,
        "copies",
        "a-remove",
        RuleFilter::True,
        "REMOVE user.stale; SET user.which = 'a'",
        3,
    )?;
    action(
        t,
        machine,
        "copies",
        "b-original",
        sql("stale = 'original'"),
        "SET user.which = 'b'",
        3,
    )?;
    Ok(())
}

fn records<S: StateStore>(
    t: &Topology,
    machine: &StateMachine<S>,
    entity: &EntityPath,
) -> Vec<MessageRecord> {
    machine
        .store()
        .scan_prefix(&keys::message_prefix(&t.namespace, entity), usize::MAX)
        .unwrap()
        .into_iter()
        .map(|(_, value)| codec::message::decode(&value).unwrap())
        .collect()
}

fn original() -> MessageContent {
    let MessageContent::Amqp(bytes) = message(
        "same-id",
        None,
        &[
            ("stale", string("original")),
            ("null", Value::Null),
            ("n", Value::Int(1)),
        ],
    ) else {
        unreachable!()
    };
    MessageContent::AmqpTextIds(bytes)
}

fn isolated_copies<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    setup(&t, &machine)?;
    let input = original();
    let bytes = match &input {
        MessageContent::AmqpTextIds(bytes) => bytes.clone(),
        _ => unreachable!(),
    };
    let (ack, entities) = t.publish(&machine, 10, input)?;
    assert_eq!(ack.as_u64(), 1);
    assert_eq!(entities, vec![t.subscription("copies")]);
    let copies = records(&t, &machine, &t.subscription("copies"));
    assert_eq!(copies.len(), 3);
    assert_eq!(
        copies
            .iter()
            .map(|m| m.sequence.as_u64())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    for copy in &copies {
        let envelope = copy.envelope.as_ref().unwrap();
        assert_eq!(envelope.as_bytes(), bytes);
        assert_eq!(copy.message_id, "same-id");
        assert!(
            envelope
                .filter_properties()
                .application_properties
                .contains_key("null")
        );
    }
    let a = copies[1].envelope.as_ref().unwrap();
    let b = copies[2].envelope.as_ref().unwrap();
    assert_eq!(a.removals(), ["stale"]);
    assert!(
        !a.filter_properties()
            .application_properties
            .contains_key("stale")
    );
    assert!(
        b.filter_properties()
            .application_properties
            .contains_key("stale")
    );
    assert_eq!(copies[0].format, MessageRecordFormat::V3);
    assert_eq!(copies[1].format, MessageRecordFormat::V4 { text_ids: true });
    validated(&machine);
    let before = image(&machine);
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(image(&machine), before);
    assert_eq!(records(&t, &machine, &t.subscription("copies")), copies);
    t.publish(&machine, 11, original())?;
    assert_eq!(records(&t, &machine, &t.subscription("copies")).len(), 6);
    validated(&machine);
    Ok(())
}
paired!(
    durable_action_copies_memory,
    durable_action_copies_fjall,
    isolated_copies
);

fn errors_and_batch<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    create_topic(&t, &machine, &t.topic)?;
    for (name, dlq) in [("errors", true), ("discard", false)] {
        t.apply(
            &machine,
            &t.topic,
            3,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new(name)?,
                config: SubscriptionConfig {
                    dead_lettering_on_filter_evaluation_exceptions: dlq,
                    ..SubscriptionConfig::default()
                },
            },
        )?;
        action(
            &t,
            &machine,
            name,
            "a-bad",
            RuleFilter::True,
            "SET user.changed = TRUE; SET user.n = 'wrong'",
            3,
        )?;
        action(
            &t,
            &machine,
            name,
            "b-filter-error",
            sql("n = 'wrong'"),
            "REMOVE user.n",
            3,
        )?;
        action(
            &t,
            &machine,
            name,
            "c-good",
            RuleFilter::True,
            "SET user.changed = TRUE",
            3,
        )?;
    }
    t.apply(
        &machine,
        &t.topic,
        10,
        CommandKind::SendContentBatch {
            messages: vec![original(), original()],
        },
    )?;
    for name in ["errors", "discard"] {
        let copies = records(&t, &machine, &t.subscription(name));
        assert_eq!(
            copies.len(),
            4,
            "actionless plus good action for each child"
        );
        assert_eq!(
            copies
                .iter()
                .filter(|c| c
                    .envelope
                    .as_ref()
                    .unwrap()
                    .filter_properties()
                    .application_properties
                    .contains_key("changed"))
                .count(),
            2
        );
    }
    let dead = records(&t, &machine, &t.subscription("errors").dead_letter_queue()?);
    assert_eq!(dead.len(), 4);
    assert_eq!(
        dead.iter()
            .filter(|c| c.dead_letter.as_ref().unwrap().reason.as_str() == "ActionException")
            .count(),
        2
    );
    for copy in dead {
        assert!(
            !copy
                .envelope
                .as_ref()
                .unwrap()
                .filter_properties()
                .application_properties
                .contains_key("changed"),
            "atomic action failure"
        );
    }
    assert!(
        records(
            &t,
            &machine,
            &t.subscription("discard").dead_letter_queue()?
        )
        .is_empty()
    );
    t.publish(&machine, 20, original())?;
    t.publish(
        &machine,
        30,
        scheduled("scheduled-error", 100, &[("n", Value::Int(1))]),
    )?;
    assert_eq!(
        records(&t, &machine, &t.subscription("errors")).len(),
        6,
        "scheduled intake is only a placeholder"
    );
    t.apply(&machine, &t.topic, 100, CommandKind::ActivateScheduled)?;
    for name in ["errors", "discard"] {
        assert_eq!(records(&t, &machine, &t.subscription(name)).len(), 8);
    }
    let dead = records(&t, &machine, &t.subscription("errors").dead_letter_queue()?);
    assert_eq!(
        dead.len(),
        8,
        "ordinary, batch and activation share the same failure policy"
    );
    assert_eq!(
        dead.iter()
            .filter(|c| c.dead_letter.as_ref().unwrap().reason.as_str() == "ActionException")
            .count(),
        4
    );
    assert!(dead.iter().all(|c| {
        !c.envelope
            .as_ref()
            .unwrap()
            .filter_properties()
            .application_properties
            .contains_key("changed")
    }));
    assert!(
        records(
            &t,
            &machine,
            &t.subscription("discard").dead_letter_queue()?
        )
        .is_empty()
    );
    let before = image(&machine);
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(image(&machine), before);
    validated(&machine);
    Ok(())
}
paired!(
    durable_action_errors_memory,
    durable_action_errors_fjall,
    errors_and_batch
);

fn failure_slots<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    create_topic(&t, &machine, &t.topic)?;
    t.apply(
        &machine,
        &t.topic,
        2,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("copies")?,
            config: SubscriptionConfig {
                dead_lettering_on_filter_evaluation_exceptions: false,
                ..SubscriptionConfig::default()
            },
        },
    )?;
    action(
        &t,
        &machine,
        "copies",
        "bad",
        RuleFilter::True,
        "SET user.n = 'wrong'",
        3,
    )?;
    let counter_key = keys::queue_counters(&t.namespace, &t.topic);
    let counter_value = codec::encode(&QueueCounters {
        next_sequence: u64::MAX - 2,
        ..QueueCounters::default()
    })?;
    machine
        .store()
        .apply(WriteBatch::default().put(counter_key.clone(), counter_value))?;
    let (ack, entities) = t.publish(&machine, 10, original())?;
    assert_eq!(ack.as_u64(), u64::MAX - 2);
    assert_eq!(entities, [t.subscription("copies")]);
    assert_eq!(
        records(&t, &machine, &t.subscription("copies")).len(),
        1,
        "discarded action consumes no extra slot"
    );
    t.apply(
        &machine,
        &t.topic,
        11,
        CommandKind::UpdateSubscription {
            name: SubscriptionName::new("copies")?,
            update: domain::SubscriptionConfigUpdate {
                dead_lettering_on_filter_evaluation_exceptions: Some(true),
                ..domain::SubscriptionConfigUpdate::default()
            },
        },
    )?;
    let counter: QueueCounters = codec::decode(&machine.store().get(&counter_key)?.unwrap())?;
    assert_eq!(
        counter.next_sequence,
        u64::MAX - 1,
        "discard reserved only the base slot"
    );
    let before = image(&machine);
    assert_eq!(
        t.publish(&machine, 12, original()),
        Err(BrokerError::SequenceNumberExhausted)
    );
    assert_eq!(
        image(&machine),
        before,
        "retained action error needs its own reserved slot; existing copy is untouched"
    );
    validated(&machine);
    Ok(())
}
paired!(
    durable_action_error_slots_memory,
    durable_action_error_slots_fjall,
    failure_slots
);

fn scheduled_and_exhaustion<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    setup(&t, &machine)?;
    let (placeholder, _) = t.publish(
        &machine,
        10,
        scheduled("later", 100, &[("stale", string("original"))]),
    )?;
    assert!(records(&t, &machine, &t.subscription("copies")).is_empty());
    t.apply(
        &machine,
        &t.subscription("copies"),
        20,
        CommandKind::DeleteRule {
            name: RuleName::new("a-remove")?,
        },
    )?;
    action(
        &t,
        &machine,
        "copies",
        "a-new",
        RuleFilter::True,
        "SET user.activated = TRUE",
        21,
    )?;
    let counter = keys::queue_counters(&t.namespace, &t.topic);
    let set_counter = |next| -> TestResult {
        machine.store().apply(WriteBatch::default().put(
            counter.clone(),
            codec::encode(&QueueCounters {
                next_sequence: next,
                ..QueueCounters::default()
            })?,
        ))?;
        Ok(())
    };
    set_counter(u64::MAX - 2)?;
    let before = image(&machine);
    assert_eq!(
        t.apply(&machine, &t.topic, 100, CommandKind::ActivateScheduled),
        Err(BrokerError::SequenceNumberExhausted)
    );
    assert_eq!(
        image(&machine),
        before,
        "no clock, placeholder, counter or index changes"
    );
    assert!(
        machine
            .message(&t.namespace, &t.topic, placeholder)?
            .is_some()
    );
    set_counter(u64::MAX - 3)?;
    t.apply(&machine, &t.topic, 100, CommandKind::ActivateScheduled)?;
    let copies = records(&t, &machine, &t.subscription("copies"));
    assert_eq!(copies.len(), 3);
    assert!(
        copies[1]
            .envelope
            .as_ref()
            .unwrap()
            .filter_properties()
            .application_properties
            .contains_key("activated")
    );
    assert!(
        machine
            .message(&t.namespace, &t.topic, placeholder)?
            .is_none()
    );
    let before = image(&machine);
    for kind in [
        CommandKind::SendContent {
            content: original(),
        },
        CommandKind::SendContentBatch {
            messages: vec![original(), original()],
        },
    ] {
        assert_eq!(
            t.apply(&machine, &t.topic, 101, kind),
            Err(BrokerError::SequenceNumberExhausted)
        );
        assert_eq!(image(&machine), before);
    }
    validated(&machine);
    Ok(())
}
paired!(
    durable_action_scheduled_memory,
    durable_action_scheduled_fjall,
    scheduled_and_exhaustion
);

fn corruption_and_raw<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    setup(&t, &machine)?;
    let before = image(&machine);
    assert!(
        action(
            &t,
            &machine,
            "copies",
            "invalid",
            RuleFilter::True,
            "SET sys.Label = 'no'",
            4
        )
        .is_err()
    );
    assert_eq!(image(&machine), before);
    assert!(matches!(
        t.apply(
            &machine,
            &t.topic,
            4,
            CommandKind::Send {
                message_id: "invalid-envelope".into(),
                body: Vec::new(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: Some(MessageEnvelope::new(vec![0xff])),
            }
        ),
        Err(BrokerError::MessageContent(
            domain::MessageContentError::Amqp(_)
        ))
    ));
    assert_eq!(
        image(&machine),
        before,
        "invalid legacy content cannot poison a durable action copy"
    );
    t.apply(
        &machine,
        &t.topic,
        10,
        CommandKind::Send {
            message_id: "raw".into(),
            body: vec![1, 2],
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )?;
    assert_eq!(records(&t, &machine, &t.subscription("copies")).len(), 1);
    let dead = records(&t, &machine, &t.subscription("copies").dead_letter_queue()?);
    assert_eq!(
        dead.len(),
        1,
        "only the matching action fails on raw content"
    );
    assert_eq!(dead[0].body, [1, 2]);
    validated(&machine);
    let bad = RuleDefinition {
        name: RuleName::new("bad")?,
        filter: RuleFilter::True,
        created_at: Timestamp::from_millis(10),
        action: Some(SqlRuleAction::new("SET sys.Label = 'no'")),
    };
    machine.store().apply(WriteBatch::default().put(
        keys::subscription_rule(&t.namespace, &t.subscription("copies"), &bad.name),
        codec::rule::encode(&bad)?,
    ))?;
    let corrupt = image(&machine);
    assert!(validate_catalog(&corrupt).is_err());
    assert!(validate_complete_state(&corrupt).is_err());
    assert!(validate_legacy_content(&corrupt).is_err());
    assert!(
        machine
            .rules(&t.namespace, &t.subscription("copies"), 0, 100)
            .is_err()
    );
    assert!(t.publish(&machine, 11, original()).is_err());
    assert_eq!(image(&machine), corrupt);
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert!(t.publish(&machine, 11, original()).is_err());
    assert_eq!(image(&machine), corrupt);
    Ok(())
}
paired!(
    durable_action_corruption_memory,
    durable_action_corruption_fjall,
    corruption_and_raw
);

fn patches_and_replacement<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    create_topic(&t, &machine, &t.topic)?;
    t.filtered(&machine, "copies", 2, &[])?;
    action(
        &t,
        &machine,
        "copies",
        "remove",
        RuleFilter::True,
        "REMOVE user.stale; REMOVE user.null",
        3,
    )?;
    t.publish(&machine, 10, original())?;
    let entity = t.subscription("copies");
    let CommandOutcome::Received(Some(delivery)) = t.apply(
        &machine,
        &entity,
        11,
        CommandKind::Receive {
            mode: domain::ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("copy")
    };
    let token = delivery.lock.unwrap().token;
    let before = image(&machine);
    assert_eq!(
        t.apply(
            &machine,
            &entity,
            12,
            CommandKind::Abandon {
                sequence: delivery.sequence,
                lock_token: token,
                replacement_envelope: original().derive()?.envelope
            }
        ),
        Err(BrokerError::MessageContent(
            domain::MessageContentError::ReplacementWouldDropOverlay
        ))
    );
    assert_eq!(image(&machine), before);
    t.apply(
        &machine,
        &entity,
        12,
        CommandKind::AbandonWithPatch {
            sequence: delivery.sequence,
            lock_token: token,
            patch: vec![
                OverlayProperty {
                    name: "extra".into(),
                    value: vec![0x41],
                },
                OverlayProperty {
                    name: "null".into(),
                    value: vec![0x40],
                },
            ],
        },
    )?;
    let copy = machine
        .message(&t.namespace, &entity, delivery.sequence)?
        .unwrap();
    let envelope = copy.envelope.as_ref().unwrap();
    assert_eq!(envelope.removals(), ["stale"]);
    assert_eq!(
        envelope.filter_properties().application_properties["null"].as_bytes(),
        [0x40]
    );
    assert!(
        !envelope
            .filter_properties()
            .application_properties
            .contains_key("stale")
    );
    assert_eq!(copy.format, MessageRecordFormat::V4 { text_ids: true });
    validated(&machine);
    let before = image(&machine);
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(image(&machine), before);
    assert_eq!(
        machine.message(&t.namespace, &entity, delivery.sequence)?,
        Some(copy)
    );
    Ok(())
}
paired!(
    durable_action_patch_memory,
    durable_action_patch_fjall,
    patches_and_replacement
);

fn id_modes_and_duplicates<P: StoreProvider>(provider: P) -> TestResult {
    let machine = StateMachine::new(provider.open()?);
    for (topic, text_ids) in [("legacy", false), ("current", true)] {
        let t = Topology {
            namespace: NamespaceName::new("tenant")?,
            topic: EntityPath::new(topic)?,
        };
        t.apply(
            &machine,
            &t.topic,
            1,
            CommandKind::CreateTopic {
                config: TopicConfig {
                    requires_duplicate_detection: true,
                    ..TopicConfig::default()
                },
            },
        )?;
        t.filtered(&machine, "copies", 1, &[("plain", RuleFilter::True)])?;
        action(
            &t,
            &machine,
            "copies",
            "remove",
            RuleFilter::True,
            "REMOVE user.stale",
            1,
        )?;
        let input = |id| -> TestResult<MessageContent> {
            let MessageContent::Amqp(bytes) =
                message("unused", None, &[("stale", string("original"))])
            else {
                unreachable!()
            };
            let mut content = message_content::decode_content(&bytes, CONTENT_LIMITS)?;
            content.properties.as_mut().unwrap().message_id = Field::Value(id);
            let bytes = encode_content(&content, CONTENT_LIMITS)?;
            Ok(if text_ids {
                MessageContent::AmqpTextIds(bytes)
            } else {
                MessageContent::Amqp(bytes)
            })
        };
        // Keep a single timestamp so setting up the second topic cannot regress
        // the shared clock. Duplicate identity intentionally remains lowercase.
        t.publish(
            &machine,
            1,
            input(MessageId::Binary(Binary {
                bytes: vec![0xab],
                width: Width::One,
            }))?,
        )?;
        t.publish(&machine, 1, input(MessageId::String(text("ab")))?)?;
        t.publish(&machine, 1, input(MessageId::String(text("AB")))?)?;
        let copies = records(&t, &machine, &t.subscription("copies"));
        assert_eq!(
            copies
                .iter()
                .map(|m| m.sequence.as_u64())
                .collect::<Vec<_>>(),
            [1, 2, 4, 5]
        );
        assert_eq!(copies[1].message_id, "ab");
        assert_eq!(copies[1].format, MessageRecordFormat::V4 { text_ids });
        assert_eq!(
            copies[1]
                .envelope
                .as_ref()
                .unwrap()
                .filter_properties()
                .message_id
                .as_deref(),
            Some(if text_ids { "AB" } else { "ab" })
        );
        assert_eq!(
            copies[0].envelope.as_ref().unwrap().as_bytes(),
            copies[1].envelope.as_ref().unwrap().as_bytes()
        );
        let counter_key = keys::queue_counters(&t.namespace, &t.topic);
        for (next, kind) in [
            (
                u64::MAX - 1,
                CommandKind::SendContent {
                    content: input(MessageId::String(text("refused")))?,
                },
            ),
            (
                u64::MAX - 3,
                CommandKind::SendContentBatch {
                    messages: vec![
                        input(MessageId::String(text("refused")))?,
                        input(MessageId::String(text("refused-later")))?,
                    ],
                },
            ),
        ] {
            machine.store().apply(WriteBatch::default().put(
                counter_key.clone(),
                codec::encode(&QueueCounters {
                    next_sequence: next,
                    ..QueueCounters::default()
                })?,
            ))?;
            let before = image(&machine);
            assert_eq!(
                t.apply(&machine, &t.topic, 9, kind),
                Err(BrokerError::SequenceNumberExhausted)
            );
            assert_eq!(
                image(&machine),
                before,
                "full plan refuses without message, history, clock or counter effects"
            );
            assert_eq!(
                machine.duplicate_history_deadline(&t.namespace, &t.topic, "refused")?,
                None
            );
        }
        validated(&machine);
    }
    Ok(())
}
paired!(
    durable_action_ids_memory,
    durable_action_ids_fjall,
    id_modes_and_duplicates
);

fn bounded_plan<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
    setup(&t, &machine)?;
    // Admission permits this native content size. Copy planning still has a
    // fixed command-wide retained-memory/work allowance, including discards.
    let mut content = message_content::decode_content(
        match &original() {
            MessageContent::AmqpTextIds(bytes) => bytes,
            _ => unreachable!(),
        },
        CONTENT_LIMITS,
    )?;
    content.body = Body::Data(vec![Binary {
        bytes: vec![0; 240 * 1024],
        width: Width::Four,
    }]);
    let input = MessageContent::AmqpTextIds(encode_content(&content, CONTENT_LIMITS)?);
    let mut seeded = WriteBatch::default();
    for index in 0..300 {
        let definition = RuleDefinition {
            name: RuleName::new(format!("extra-{index:03}"))?,
            filter: RuleFilter::True,
            action: Some(SqlRuleAction::new("REMOVE user.stale")),
            created_at: Timestamp::from_millis(3),
        };
        seeded.push_put(
            keys::subscription_rule(&t.namespace, &t.subscription("copies"), &definition.name),
            codec::rule::encode(&definition)?,
        );
    }
    machine.store().apply(seeded)?;
    let before = image(&machine);
    assert_eq!(
        t.publish(&machine, 10, input.clone()),
        Err(BrokerError::RuleConfig(RuleConfigError::SqlActionPlanLimit))
    );
    assert_eq!(image(&machine), before);
    let MessageContent::AmqpTextIds(bytes) = input else {
        unreachable!()
    };
    let mut scheduled_content = message_content::decode_content(&bytes, CONTENT_LIMITS)?;
    scheduled_content.message_annotations = Some(vec![(
        AnnotationKey::Symbol(Symbol {
            text: "x-opt-scheduled-enqueue-time".into(),
            width: Width::One,
        }),
        Value::Timestamp(100),
    )]);
    t.publish(
        &machine,
        10,
        MessageContent::AmqpTextIds(encode_content(&scheduled_content, CONTENT_LIMITS)?),
    )?;
    let before = image(&machine);
    assert_eq!(
        t.apply(&machine, &t.topic, 100, CommandKind::ActivateScheduled),
        Err(BrokerError::RuleConfig(RuleConfigError::SqlActionPlanLimit))
    );
    assert_eq!(image(&machine), before);
    Ok(())
}
paired!(
    durable_action_plan_bound_memory,
    durable_action_plan_bound_fjall,
    bounded_plan
);

#[test]
fn deletion_codec_is_not_null_and_older_encoders_refuse_it() -> TestResult {
    let input = original().derive()?;
    let envelope = input
        .envelope
        .unwrap()
        .with_removals(vec!["stale".into()])
        .with_overlay(vec![OverlayProperty {
            name: "null".into(),
            value: vec![0x40],
        }]);
    let mut record = MessageRecord {
        sequence: SequenceNumber::new(7),
        message_id: "same-id".into(),
        body: Vec::new(),
        enqueued_at: Timestamp::from_millis(1),
        scheduled_enqueue_at: None,
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: None,
        envelope: Some(envelope),
        format: MessageRecordFormat::V4 { text_ids: true },
    };
    let bytes = codec::message::encode(&record)?;
    assert_eq!(bytes[0], 4);
    assert_eq!(codec::message::decode(&bytes)?, record);
    for old in [
        MessageRecordFormat::V1,
        MessageRecordFormat::V2,
        MessageRecordFormat::V3,
    ] {
        record.format = old;
        assert!(codec::message::encode(&record).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(codec::message::decode(&trailing).is_err());
    record.format = MessageRecordFormat::V4 { text_ids: true };
    record.envelope = Some(record.envelope.unwrap().with_removals(vec!["null".into()]));
    assert!(
        codec::message::encode(&record).is_err(),
        "set/remove collision"
    );
    Ok(())
}
