use super::*;

fn replacement_projection<P: StoreProvider>(provider: P) -> TestResult {
    let mut machine = StateMachine::new(provider.open()?);
    for (case, text_ids, defer) in [
        (0, false, false),
        (1, false, true),
        (2, true, false),
        (3, true, true),
    ] {
        let base = 20 * case + 1;
        let t = Topology {
            namespace: NamespaceName::new("tenant")?,
            topic: EntityPath::new(format!("replacement-{case}"))?,
        };
        t.apply(
            &machine,
            &t.topic,
            base,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        t.filtered(&machine, "copies", base + 1, &[])?;
        action(
            &t,
            &machine,
            "copies",
            "fails",
            RuleFilter::True,
            "SET user.n = 'wrong'",
            base + 1,
        )?;
        let MessageContent::AmqpTextIds(bytes) = original() else {
            unreachable!()
        };
        let input = if text_ids {
            MessageContent::AmqpTextIds(bytes.clone())
        } else {
            MessageContent::Amqp(bytes.clone())
        };
        t.publish(&machine, base + 2, input)?;
        let entity = t.subscription("copies").dead_letter_queue()?;
        let CommandOutcome::Received(Some(delivery)) = t.apply(
            &machine,
            &entity,
            base + 3,
            CommandKind::Receive {
                mode: domain::ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        )?
        else {
            panic!("action error copy")
        };
        let token = delivery.lock.unwrap().token;
        let stored = machine
            .message(&t.namespace, &entity, delivery.sequence)?
            .unwrap();
        assert_eq!(stored.format, MessageRecordFormat::V4 { text_ids });
        assert!(stored.envelope.as_ref().unwrap().overlay().is_empty());
        assert!(stored.envelope.as_ref().unwrap().removals().is_empty());
        let command = |envelope| {
            Command::new(
                t.namespace.clone(),
                entity.clone(),
                Timestamp::from_millis(base + 4),
                if defer {
                    CommandKind::Defer {
                        sequence: delivery.sequence,
                        lock_token: token,
                        replacement_envelope: Some(envelope),
                    }
                } else {
                    CommandKind::Abandon {
                        sequence: delivery.sequence,
                        lock_token: token,
                        replacement_envelope: Some(envelope),
                    }
                },
            )
        };
        let before = image(&machine);
        let invalid = DurableProposal::decode(
            &DurableProposal::unbound(command(MessageEnvelope::new(vec![0xff]))).encode()?,
        )?;
        assert!(matches!(
            machine.apply(invalid.command()),
            Err(BrokerError::MessageContent(
                domain::MessageContentError::Amqp(_)
            ))
        ));
        assert_eq!(
            image(&machine),
            before,
            "invalid replacement leaves lock, clock and content untouched"
        );

        let mut content = message_content::decode_content(&bytes, CONTENT_LIMITS)?;
        content.properties.as_mut().unwrap().message_id = Field::Value(MessageId::Binary(Binary {
            bytes: vec![0xab],
            width: Width::One,
        }));
        content.application_properties = Some(vec![(text("replacement"), Value::Bool(true))]);
        let bytes = encode_content(&content, CONTENT_LIMITS)?;
        let expected = if text_ids {
            MessageContent::AmqpTextIds(bytes.clone())
        } else {
            MessageContent::Amqp(bytes.clone())
        }
        .derive()?
        .envelope
        .unwrap();
        let replacement = MessageEnvelope::new(bytes.clone());
        assert_eq!(
            replacement.filter_properties(),
            &FilterProperties::default()
        );
        let captured =
            DurableProposal::decode(&DurableProposal::unbound(command(replacement)).encode()?)?;
        machine.apply(captured.command())?;
        let copy = machine
            .message(&t.namespace, &entity, delivery.sequence)?
            .unwrap();
        assert_eq!(copy.format, MessageRecordFormat::V4 { text_ids });
        assert_eq!(
            copy.state,
            if defer {
                MessageState::Deferred
            } else {
                MessageState::Ready
            }
        );
        assert_eq!(copy.envelope.as_ref(), Some(&expected));
        assert_eq!(
            copy.envelope
                .as_ref()
                .unwrap()
                .filter_properties()
                .message_id
                .as_deref(),
            Some(if text_ids { "AB" } else { "ab" })
        );
        validated(&machine);
        let before = image(&machine);
        drop(machine);
        machine = StateMachine::new(provider.open()?);
        assert_eq!(image(&machine), before);
        assert_eq!(
            machine.message(&t.namespace, &entity, delivery.sequence)?,
            Some(copy)
        );
        validated(&machine);
    }
    Ok(())
}

paired!(
    v4_error_replacements_derive_both_id_modes_memory,
    v4_error_replacements_derive_both_id_modes_fjall,
    replacement_projection
);

fn discarded_filter_budget<P: StoreProvider>(provider: P) -> TestResult {
    let t = Topology::new()?;
    let machine = StateMachine::new(provider.open()?);
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
    t.apply(
        &machine,
        &t.subscription("copies"),
        2,
        CommandKind::DeleteRule {
            name: RuleName::new(DEFAULT_RULE_NAME)?,
        },
    )?;
    let mut batch = WriteBatch::default();
    for index in 0..MAX_TOPIC_SQL_RULES {
        let definition = RuleDefinition {
            name: RuleName::new(format!("fails-{index:04}"))?,
            filter: sql("n = 'wrong'"),
            action: Some(SqlRuleAction::new("REMOVE user.n")),
            created_at: Timestamp::from_millis(2),
        };
        batch.push_put(
            keys::subscription_rule(&t.namespace, &t.subscription("copies"), &definition.name),
            codec::rule::encode(&definition)?,
        );
    }
    machine.store().apply(batch)?;
    let before = image(&machine);
    let proposed = DurableProposal::unbound(Command::new(
        t.namespace.clone(),
        t.topic.clone(),
        Timestamp::from_millis(10),
        CommandKind::SendContentBatch {
            messages: ["one", "two", "three"]
                .into_iter()
                .map(|id| message(id, None, &[("n", Value::Int(1))]))
                .collect(),
        },
    ));
    let captured = DurableProposal::decode(&proposed.encode()?)?;
    assert_eq!(
        machine.apply(captured.command()),
        Err(BrokerError::RuleConfig(RuleConfigError::SqlActionPlanLimit))
    );
    assert_eq!(
        image(&machine),
        before,
        "discarded action-filter errors must charge before any writes"
    );
    for id in ["one", "two", "three"] {
        assert_eq!(
            machine.duplicate_history_deadline(&t.namespace, &t.topic, id)?,
            None
        );
    }

    let mut repair = WriteBatch::default();
    for index in 999..MAX_TOPIC_SQL_RULES {
        repair.push_delete(keys::subscription_rule(
            &t.namespace,
            &t.subscription("copies"),
            &RuleName::new(format!("fails-{index:04}"))?,
        ));
    }
    repair.push_put(
        keys::queue_counters(&t.namespace, &t.topic),
        codec::encode(&QueueCounters {
            next_sequence: u64::MAX - 1,
            ..QueueCounters::default()
        })?,
    );
    machine.store().apply(repair)?;
    let (ack, populated) = t.publish(
        &machine,
        10,
        message("after", None, &[("n", Value::Int(1))]),
    )?;
    assert_eq!(ack.as_u64(), u64::MAX - 1);
    assert!(populated.is_empty());
    assert!(records(&t, &machine, &t.subscription("copies")).is_empty());
    assert!(records(&t, &machine, &t.subscription("copies").dead_letter_queue()?).is_empty());
    let counters: QueueCounters = codec::decode(
        &machine
            .store()
            .get(&keys::queue_counters(&t.namespace, &t.topic))?
            .unwrap(),
    )?;
    assert_eq!(
        counters.next_sequence,
        u64::MAX,
        "discarded filter errors reserve no extra slots"
    );
    validated(&machine);
    let before = image(&machine);
    drop(machine);
    let machine = StateMachine::new(provider.open()?);
    assert_eq!(image(&machine), before);
    validated(&machine);
    Ok(())
}

paired!(
    discarded_action_filter_errors_share_plan_budget_memory,
    discarded_action_filter_errors_share_plan_budget_fjall,
    discarded_filter_budget
);
