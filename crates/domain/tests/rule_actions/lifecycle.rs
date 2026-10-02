use super::*;

fn filters_use_original_and_action_matches_are_independent_copies<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    add(
        &fixture,
        "child",
        "a-remove",
        RuleFilter::True,
        Some("REMOVE color"),
        0,
    )?;
    add(
        &fixture,
        "child",
        "b-match",
        RuleFilter::Correlation(CorrelationFilter {
            properties: BTreeMap::from([("color".into(), MessageValue::String("Red".into()))]),
            ..CorrelationFilter::default()
        }),
        Some("REMOVE number"),
        0,
    )?;
    add(
        &fixture,
        "child",
        "ordinary",
        RuleFilter::Sql(SqlFilter::new("color = 'Red'")?),
        None,
        0,
    )?;
    add(
        &fixture,
        "child",
        "never",
        RuleFilter::False,
        Some("REMOVE color"),
        0,
    )?;
    let original = member("one");
    let result = publish(&fixture, 1, vec![original.clone()])?;
    assert_eq!(
        result.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(1)]
        }
    );
    effects(&result, std::slice::from_ref(&child));
    let copies = records(&fixture, &child)?;
    assert_eq!(copies.len(), 3);
    assert_eq!(copies[0].sequence, SequenceNumber::new(1));
    assert_eq!(copies[0].envelope.as_deref(), Some(&original.envelope));
    for (sequence, name, removed) in [(2, "a-remove", "color"), (3, "b-match", "number")] {
        let copy = record(&fixture, &child, sequence)?.expect("one copy for each matching action");
        let mut expected = original.envelope.clone();
        expected.application_properties.remove(removed);
        expected
            .application_properties
            .insert("RuleName".into(), MessageValue::String(name.into()));
        assert_eq!(copy.envelope.as_deref(), Some(&expected));
        assert_eq!(copy.body, original.body);
        assert_eq!(copy.message_id, original.message_id);
        assert_eq!(copy.state, MessageState::Ready);
    }
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("parent allocator")
            .next_sequence,
        4
    );
    assert_eq!(counters(&fixture, &child)?, None);
    assert!(record(&fixture, &fixture.entity, 1)?.is_none());
    Ok(())
}

fn exact_key_removal_and_final_rule_name_preserve_rich_message_metadata<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            default_time_to_live_millis: Some(80),
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(
        &fixture,
        "child",
        SubscriptionConfig {
            default_time_to_live_millis: Some(50),
            ..SubscriptionConfig::default()
        },
    )?;
    remove(&fixture, "child", "$Default", 0)?;
    let source = "  REMOVE user.[color]; REMOVE [RuleName]; REMOVE missing; ";
    add(
        &fixture,
        "child",
        "Exact Rule",
        RuleFilter::True,
        Some(source),
        0,
    )?;
    let mut original = member("rich");
    original.time_to_live_millis = Some(30);
    original.session_id = Some(SessionId::new("cart")?);
    original.envelope.header = Some(domain::MessageHeader {
        durable: true,
        priority: 7,
        first_acquirer: false,
    });
    original
        .envelope
        .application_properties
        .insert("Color".into(), MessageValue::String("capital".into()));
    original
        .envelope
        .application_properties
        .insert("RuleName".into(), MessageValue::String("producer".into()));
    original
        .envelope
        .application_properties
        .insert("rulename".into(), MessageValue::String("lower".into()));
    original.envelope.message_annotations.insert(
        domain::AnnotationKey::Symbol("producer".into()),
        MessageValue::List(vec![
            MessageValue::Long(42),
            MessageValue::String("annotation".into()),
        ]),
    );
    original.envelope.footer.insert(
        domain::AnnotationKey::Ulong(7),
        MessageValue::Binary(vec![1, 2, 3]),
    );
    original.envelope.body = MessageBody::Sequence(vec![vec![
        MessageValue::String("typed".into()),
        MessageValue::Int(42),
    ]]);
    let mut expected = original.envelope.clone();
    expected.application_properties.remove("color");
    expected
        .application_properties
        .insert("RuleName".into(), MessageValue::String("Exact Rule".into()));
    publish(&fixture, 10, vec![original.clone()])?;
    assert!(record(&fixture, &child, 1)?.is_none());
    let copy = record(&fixture, &child, 2)?.expect("action-only copy");
    assert_eq!(copy.envelope.as_deref(), Some(&expected));
    assert_eq!(copy.body, original.body);
    assert_eq!(copy.message_id, "rich");
    assert_eq!(copy.session_id, original.session_id);
    assert_eq!(copy.enqueued_at, Timestamp::from_millis(10));
    assert_eq!(copy.expires_at, Some(Timestamp::from_millis(40)));
    assert_eq!(copy.scheduled_enqueue_time, None);
    assert!(copy.dead_letter.is_none());
    let stored = rules(&fixture, "child")?;
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0]
            .action
            .as_ref()
            .expect("source stored")
            .expression(),
        source
    );
    Ok(())
}

fn legacy_send_materializes_only_action_copies_and_allocates_parent_sequences<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    add(
        &fixture,
        "child",
        "action",
        RuleFilter::True,
        Some("REMOVE absent"),
        0,
    )?;
    let result = apply(
        &fixture,
        1,
        CommandKind::Send {
            message_id: "legacy".into(),
            body: b"legacy-body".to_vec(),
            time_to_live_millis: None,
            session_id: None,
        },
    )?;
    assert_eq!(
        result.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    effects(&result, std::slice::from_ref(&child));
    let base = record(&fixture, &child, 1)?.expect("legacy OR copy");
    assert_eq!(base.envelope, None);
    let action = record(&fixture, &child, 2)?.expect("materialized action copy");
    let envelope = action.envelope.expect("typed envelope");
    assert_eq!(
        envelope.properties.message_id,
        Some(MessageIdentifier::String("legacy".into()))
    );
    assert_eq!(
        envelope.body,
        MessageBody::Data(vec![b"legacy-body".to_vec()])
    );
    assert_eq!(
        envelope.application_properties,
        BTreeMap::from([("RuleName".into(), MessageValue::String("action".into()))])
    );
    assert_eq!(action.body, base.body);
    assert_eq!(action.message_id, base.message_id);
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("parent counter")
            .next_sequence,
        3
    );
    Ok(())
}

fn all_batch_bases_precede_action_sequences_and_duplicates_have_no_action_copies<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    for name in ["a", "b"] {
        add(
            &fixture,
            "child",
            name,
            RuleFilter::True,
            Some("REMOVE missing"),
            0,
        )?;
    }
    let result = publish(
        &fixture,
        1,
        vec![member("known"), member("known"), member("fresh")],
    )?;
    assert_eq!(
        result.outcome,
        CommandOutcome::BatchSent {
            sequences: (1..=3).map(SequenceNumber::new).collect()
        }
    );
    let copies = records(&fixture, &child)?;
    assert_eq!(
        copies
            .iter()
            .map(|copy| (copy.sequence, copy.message_id.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (SequenceNumber::new(1), "known"),
            (SequenceNumber::new(3), "fresh"),
            (SequenceNumber::new(4), "known"),
            (SequenceNumber::new(5), "known"),
            (SequenceNumber::new(6), "fresh"),
            (SequenceNumber::new(7), "fresh"),
        ]
    );
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("all input bases first")
            .next_sequence,
        8
    );
    let duplicate = publish(&fixture, 2, vec![member("known")])?;
    effects(&duplicate, &[]);
    assert_eq!(records(&fixture, &child)?, copies);
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("duplicate base only")
            .next_sequence,
        9
    );
    Ok(())
}

fn missing_session_is_dead_lettered_for_each_action_copy<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let required = subscribe(
        &fixture,
        "required",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
    )?;
    let healthy = subscribe(&fixture, "healthy", SubscriptionConfig::default())?;
    for name in ["a", "b"] {
        add(
            &fixture,
            "required",
            name,
            RuleFilter::True,
            Some("REMOVE color"),
            0,
        )?;
    }
    let original = member("anonymous");
    let result = publish(&fixture, 1, vec![original.clone()])?;
    let shadow = required.dead_letter_queue()?;
    effects(&result, &[healthy.clone(), shadow.clone()]);
    assert!(records(&fixture, &required)?.is_empty());
    let copies = records(&fixture, &shadow)?;
    assert_eq!(copies.len(), 3);
    for (index, copy) in copies.iter().enumerate() {
        assert_eq!(copy.sequence, SequenceNumber::new(index as u64 + 1));
        assert_eq!(copy.session_id, None);
        assert_eq!(copy.expires_at, None);
        assert_eq!(
            copy.dead_letter.as_ref().expect("per-copy metadata").reason,
            DeadLetterReason::MissingSessionId
        );
        assert_eq!(
            copy.dead_letter
                .as_ref()
                .expect("per-copy metadata")
                .dead_lettered_at,
            Timestamp::from_millis(1)
        );
        assert_eq!(copy.body, original.body);
        if index != 0 {
            let mut expected = original.envelope.clone();
            expected.application_properties.remove("color");
            expected.application_properties.insert(
                "RuleName".into(),
                MessageValue::String(if index == 1 { "a" } else { "b" }.into()),
            );
            assert_eq!(copy.envelope.as_deref(), Some(&expected));
        }
    }
    assert_eq!(copies[0].envelope.as_deref(), Some(&original.envelope));
    assert_eq!(peek(&fixture, &healthy, 1)?.len(), 1);
    assert_eq!(counters(&fixture, &shadow)?, None);
    Ok(())
}

fn filter_error_suppresses_all_action_and_non_action_matches_in_that_subscription<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let enabled = subscribe(&fixture, "enabled", SubscriptionConfig::default())?;
    let disabled = subscribe(
        &fixture,
        "disabled",
        SubscriptionConfig {
            dead_lettering_on_filter_evaluation_exceptions: false,
            ..SubscriptionConfig::default()
        },
    )?;
    let healthy = subscribe(&fixture, "healthy", SubscriptionConfig::default())?;
    for name in ["enabled", "disabled"] {
        add(
            &fixture,
            name,
            "a-action",
            RuleFilter::True,
            Some("REMOVE color"),
            0,
        )?;
        add(
            &fixture,
            name,
            "m-failure",
            RuleFilter::Sql(SqlFilter::new("1 / 0 = 1")?),
            Some("REMOVE number"),
            0,
        )?;
        add(
            &fixture,
            name,
            "n-second-failure",
            RuleFilter::Sql(SqlFilter::new("number = 'secret'")?),
            None,
            0,
        )?;
        add(
            &fixture,
            name,
            "z-action",
            RuleFilter::True,
            Some("REMOVE number"),
            0,
        )?;
    }
    let original = member("failure");
    let result = publish(&fixture, 1, vec![original.clone()])?;
    let shadow = enabled.dead_letter_queue()?;
    effects(&result, &[shadow.clone(), healthy.clone()]);
    let copies = records(&fixture, &shadow)?;
    assert_eq!(copies.len(), 1);
    assert_eq!(copies[0].sequence, SequenceNumber::new(1));
    assert_eq!(copies[0].envelope.as_deref(), Some(&original.envelope));
    assert_eq!(
        copies[0]
            .dead_letter
            .as_ref()
            .expect("first filter error")
            .reason,
        DeadLetterReason::Application("SwitchyardSqlFilterError".into())
    );
    assert_eq!(
        copies[0]
            .dead_letter
            .as_ref()
            .expect("first sorted error wins")
            .description,
        "SQL filter integer arithmetic divided by zero."
    );
    assert!(records(&fixture, &enabled)?.is_empty());
    assert!(records(&fixture, &disabled)?.is_empty());
    assert!(records(&fixture, &disabled.dead_letter_queue()?)?.is_empty());
    assert_eq!(records(&fixture, &healthy)?.len(), 1);
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("no suppressed action allocations")
            .next_sequence,
        2
    );
    Ok(())
}

for_each_backend! {
    filters_use_original_and_action_matches_are_independent_copies,
    exact_key_removal_and_final_rule_name_preserve_rich_message_metadata,
    legacy_send_materializes_only_action_copies_and_allocates_parent_sequences,
    all_batch_bases_precede_action_sequences_and_duplicates_have_no_action_copies,
    missing_session_is_dead_lettered_for_each_action_copy,
    filter_error_suppresses_all_action_and_non_action_matches_in_that_subscription,
}
