use super::*;

fn large_topic<P: StoreProvider>(provider: P) -> TestResult<QueueFixture<P>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "anchor")?;
    fixture.entity = EntityPath::new("orders")?;
    fixture.at(
        0,
        CommandKind::CreateTopic {
            config: TopicConfig {
                requires_duplicate_detection: true,
                max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
                ..TopicConfig::default()
            },
        },
    )?;
    Ok(fixture)
}

fn large_subscription<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    name: &str,
    requires_session: bool,
) -> TestResult<EntityPath> {
    let name = SubscriptionName::new(name)?;
    fixture.at(
        0,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig {
                requires_session,
                max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
                ..SubscriptionConfig::default()
            },
        },
    )?;
    Ok(fixture.entity.subscription(&name)?)
}

fn fanout_error(limit: IngressBatchLimit, maximum: usize) -> BrokerError {
    BrokerError::TopicFanoutTooLarge { limit, maximum }
}

fn retained_active_bytes_include_the_normalized_session_identifier<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = large_topic(provider)?;
    let plain = large_subscription(&fixture, "plain", false)?;
    let required = large_subscription(&fixture, "required", true)?;
    let session_id = "s".repeat(domain::MAX_SESSION_ID_BYTES);
    let mut exact = member("known", Some(&session_id));
    exact.envelope = MessageEnvelope::default();
    exact.body = vec![
        0;
        MAX_TOPIC_FANOUT_CONTENT_BYTES / 2
            - exact.envelope.content_size()
            - exact.message_id.len()
            - session_id.len()
    ];
    let mut excess = exact.clone();
    excess.body.push(0);
    reject(
        &fixture,
        10,
        rich(excess.clone()),
        fanout_error(
            IngressBatchLimit::ContentBytes,
            MAX_TOPIC_FANOUT_CONTENT_BYTES,
        ),
    )?;
    effects(
        &apply(&fixture, 1, rich(exact.clone()))?,
        vec![plain.clone(), required.clone()],
    );
    effects(&apply(&fixture, 2, rich(excess))?, vec![]);
    for entity in [&plain, &required] {
        let copy = fixture
            .machine
            .message(&fixture.namespace, entity, SequenceNumber::new(1))?
            .expect("exact active copy");
        assert_eq!(copy.body.len(), exact.body.len());
        assert_eq!(
            copy.session_id.as_ref().expect("session property").as_str(),
            session_id
        );
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, entity, SequenceNumber::new(2))?,
            None
        );
    }
    Ok(())
}

fn retained_null_copy_bytes_include_the_canonical_reason_and_description<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = large_topic(provider)?;
    let first = large_subscription(&fixture, "first", true)?;
    let second = large_subscription(&fixture, "second", true)?;
    let shadows = [first.dead_letter_queue()?, second.dead_letter_queue()?];
    let mut exact = member("known", None);
    exact.envelope = MessageEnvelope::default();
    exact.body = vec![
        0;
        MAX_TOPIC_FANOUT_CONTENT_BYTES / 2
            - exact.envelope.content_size()
            - exact.message_id.len()
            - "Session ID is null".len()
            - MISSING_SESSION_DESCRIPTION.len()
    ];
    let mut excess = exact.clone();
    excess.body.push(0);
    reject(
        &fixture,
        10,
        rich(excess.clone()),
        fanout_error(
            IngressBatchLimit::ContentBytes,
            MAX_TOPIC_FANOUT_CONTENT_BYTES,
        ),
    )?;
    effects(&apply(&fixture, 1, rich(exact.clone()))?, shadows.to_vec());
    effects(&apply(&fixture, 2, rich(excess))?, vec![]);
    for shadow in &shadows {
        let copy = fixture
            .machine
            .message(&fixture.namespace, shadow, SequenceNumber::new(1))?
            .expect("exact SDLQ copy");
        assert_eq!(copy.body.len(), exact.body.len());
        assert_eq!(
            copy.dead_letter.as_ref().expect("reason").reason,
            DeadLetterReason::MissingSessionId
        );
        assert_eq!(
            copy.dead_letter.as_ref().expect("reason").description,
            MISSING_SESSION_DESCRIPTION
        );
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, shadow, SequenceNumber::new(2))?,
            None
        );
    }
    Ok(())
}

fn retained_null_copies_reserve_two_projected_value_nodes<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = large_topic(provider)?;
    let first = large_subscription(&fixture, "first", true)?;
    let second = large_subscription(&fixture, "second", true)?;
    let shadows = [first.dead_letter_queue()?, second.dead_letter_queue()?];
    let children = MAX_TOPIC_FANOUT_VALUE_ITEMS / 2 - 3;
    let mut exact = member("array", None);
    exact.envelope.application_properties.clear();
    exact.envelope.body =
        MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; children]));
    exact.envelope.validate()?;
    assert_eq!(children + 1, MAX_TOPIC_FANOUT_VALUE_ITEMS / 2 - 2);
    let mut excess = exact.clone();
    excess.envelope.body =
        MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; children + 1]));
    reject(
        &fixture,
        10,
        rich(excess.clone()),
        fanout_error(IngressBatchLimit::ValueItems, MAX_TOPIC_FANOUT_VALUE_ITEMS),
    )?;
    effects(&apply(&fixture, 1, rich(exact))?, shadows.to_vec());
    effects(&apply(&fixture, 2, rich(excess))?, vec![]);
    for shadow in &shadows {
        let copy = fixture
            .machine
            .message(&fixture.namespace, shadow, SequenceNumber::new(1))?
            .expect("exact SDLQ nodes");
        let envelope = copy.envelope.as_ref().expect("envelope");
        envelope.validate()?;
        let MessageBody::Value(MessageValue::Array(values)) = &envelope.body else {
            panic!("retained array")
        };
        assert_eq!(values.len(), children);
        assert!(envelope.application_properties.is_empty());
        let info = copy.dead_letter.as_ref().expect("projected metadata");
        assert_eq!(info.reason, DeadLetterReason::MissingSessionId);
        assert_eq!(info.description, MISSING_SESSION_DESCRIPTION);
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, shadow, SequenceNumber::new(2))?,
            None
        );
    }
    Ok(())
}

fn mixed_session_and_null_routes_share_the_exact_copy_cap<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, false)?;
    let plain = subscribe(&fixture, "plain", false)?;
    let required = subscribe(&fixture, "required", true)?;
    let shadow = required.dead_letter_queue()?;
    let count = MAX_TOPIC_FANOUT_COPIES / 2;
    let messages = |count| {
        (0..count)
            .map(|index| {
                member(
                    &format!("id-{index}"),
                    if index % 2 == 0 { Some("cart") } else { None },
                )
            })
            .collect()
    };
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: messages(count + 1),
        },
        fanout_error(IngressBatchLimit::Messages, MAX_TOPIC_FANOUT_COPIES),
    )?;
    let application = apply(
        &fixture,
        1,
        CommandKind::SendBatch {
            messages: messages(count),
        },
    )?;
    effects(
        &application,
        vec![plain.clone(), required.clone(), shadow.clone()],
    );
    for (entity, expected) in [
        (&plain, count),
        (&required, count / 2),
        (&shadow, count / 2),
    ] {
        assert_eq!(
            fixture
                .machine
                .store()
                .scan_prefix(&keys::message_prefix(&fixture.namespace, entity), count + 1)?
                .len(),
            expected
        );
        assert_eq!(counters(&fixture, entity)?, None);
    }
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("topic counter")
            .next_sequence,
        count as u64 + 1
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}
for_each_backend! {
    retained_active_bytes_include_the_normalized_session_identifier,
    retained_null_copy_bytes_include_the_canonical_reason_and_description,
    retained_null_copies_reserve_two_projected_value_nodes,
    mixed_session_and_null_routes_share_the_exact_copy_cap,
}
