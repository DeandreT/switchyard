use super::*;

fn action_copies_share_the_exact_retained_copy_ceiling<P: StoreProvider>(
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
    let inputs = MAX_TOPIC_FANOUT_COPIES / 2;
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: (0..=inputs)
                .map(|index| member(&format!("id-{index}")))
                .collect(),
        },
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::Messages,
            maximum: MAX_TOPIC_FANOUT_COPIES,
        },
    )?;
    let result = publish(
        &fixture,
        1,
        (0..inputs)
            .map(|index| member(&format!("id-{index}")))
            .collect(),
    )?;
    effects(&result, std::slice::from_ref(&child));
    let copies = records(&fixture, &child)?;
    assert_eq!(copies.len(), MAX_TOPIC_FANOUT_COPIES);
    for index in 0..inputs {
        assert_eq!(
            copies[index].sequence,
            SequenceNumber::new(index as u64 + 1)
        );
        assert_eq!(
            copies[index + inputs].sequence,
            SequenceNumber::new((index + inputs) as u64 + 1)
        );
        assert_eq!(copies[index].message_id, copies[index + inputs].message_id);
        assert!(
            !copies[index]
                .envelope
                .as_ref()
                .expect("base copy")
                .application_properties
                .contains_key("RuleName")
        );
        assert_eq!(
            copies[index + inputs]
                .envelope
                .as_ref()
                .expect("action copy")
                .application_properties
                .get("RuleName"),
            Some(&MessageValue::String("action".into()))
        );
    }
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("all copied sequences")
            .next_sequence,
        MAX_TOPIC_FANOUT_COPIES as u64 + 1
    );
    assert_eq!(counters(&fixture, &child)?, None);
    Ok(())
}

fn transformed_content_including_rule_name_has_an_exact_shared_byte_ceiling<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..TopicConfig::default()
        },
    )?;
    let child = subscribe(
        &fixture,
        "child",
        SubscriptionConfig {
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..SubscriptionConfig::default()
        },
    )?;
    remove(&fixture, "child", "$Default", 0)?;
    for name in ["a", "b"] {
        add(
            &fixture,
            "child",
            name,
            RuleFilter::True,
            Some("REMOVE absent"),
            0,
        )?;
    }
    let mut exact = member("");
    exact.envelope = MessageEnvelope::default();
    let mut projected = exact.envelope.clone();
    projected
        .application_properties
        .insert("RuleName".into(), MessageValue::String("a".into()));
    exact.body = vec![0; MAX_TOPIC_FANOUT_CONTENT_BYTES / 2 - projected.content_size()];
    assert_eq!(
        2 * (projected.content_size() + exact.body.len()),
        MAX_TOPIC_FANOUT_CONTENT_BYTES
    );
    let mut excess = exact.clone();
    excess.body.push(0);
    reject(
        &fixture,
        10,
        rich(excess),
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ContentBytes,
            maximum: MAX_TOPIC_FANOUT_CONTENT_BYTES,
        },
    )?;
    effects(
        &apply(&fixture, 1, rich(exact.clone()))?,
        std::slice::from_ref(&child),
    );
    let copies = records(&fixture, &child)?;
    assert_eq!(copies.len(), 2);
    assert_eq!(
        copies
            .iter()
            .map(|copy| copy
                .envelope
                .as_ref()
                .expect("action content")
                .content_size()
                + copy.body.len()
                + copy.message_id.len())
            .sum::<usize>(),
        MAX_TOPIC_FANOUT_CONTENT_BYTES
    );
    for copy in copies {
        assert_eq!(copy.body, exact.body);
    }
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("base then action sequences")
            .next_sequence,
        4
    );
    Ok(())
}

fn final_annotation_nodes_are_charged_per_copy_at_the_exact_value_ceiling<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "child", SubscriptionConfig::default())?;
    remove(&fixture, "child", "$Default", 0)?;
    for name in ["a", "b"] {
        add(
            &fixture,
            "child",
            name,
            RuleFilter::True,
            Some("REMOVE absent"),
            0,
        )?;
    }
    let children = MAX_TOPIC_FANOUT_VALUE_ITEMS / 2 - 2;
    let mut exact = member("");
    exact.body.clear();
    exact.envelope = MessageEnvelope {
        body: MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; children])),
        ..MessageEnvelope::default()
    };
    let mut excess = exact.clone();
    excess.envelope.body =
        MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; children + 1]));
    reject(
        &fixture,
        10,
        rich(excess),
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ValueItems,
            maximum: MAX_TOPIC_FANOUT_VALUE_ITEMS,
        },
    )?;
    let result = apply(&fixture, 1, rich(exact))?;
    effects(&result, std::slice::from_ref(&child));
    let copies = records(&fixture, &child)?;
    assert_eq!(copies.len(), 2);
    for copy in copies {
        let envelope = copy.envelope.expect("typed action copy");
        let MessageBody::Value(MessageValue::Array(values)) = envelope.body else {
            panic!("array body")
        };
        assert_eq!(values.len(), children);
        assert_eq!(envelope.application_properties.len(), 1);
        assert!(matches!(
            envelope.application_properties.get("RuleName"),
            Some(MessageValue::String(_))
        ));
    }
    assert_eq!(2 * (children + 2), MAX_TOPIC_FANOUT_VALUE_ITEMS);
    Ok(())
}

for_each_backend! {
    action_copies_share_the_exact_retained_copy_ceiling,
    transformed_content_including_rule_name_has_an_exact_shared_byte_ceiling,
    final_annotation_nodes_are_charged_per_copy_at_the_exact_value_ceiling,
}
