use super::*;

fn retained_copy_count_has_an_exact_boundary_before_any_commit<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    let count = MAX_TOPIC_FANOUT_COPIES / 2;
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: (0..=count)
                .map(|index| member(&format!("id-{index}")))
                .collect(),
        },
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::Messages,
            maximum: MAX_TOPIC_FANOUT_COPIES,
        },
    )?;
    let application = apply(
        &fixture,
        1,
        CommandKind::SendBatch {
            messages: (0..count)
                .map(|index| member(&format!("id-{index}")))
                .collect(),
        },
    )?;
    effects(&application, &[alpha.clone(), beta.clone()]);
    assert_eq!(
        fixture
            .machine
            .store()
            .scan_prefix(&keys::message_prefix(&fixture.namespace, &alpha), count + 1)?
            .len(),
        count
    );
    assert_eq!(
        fixture
            .machine
            .store()
            .scan_prefix(&keys::message_prefix(&fixture.namespace, &beta), count + 1)?
            .len(),
        count
    );
    assert_eq!(counters(&fixture, &alpha)?, None);
    assert_eq!(counters(&fixture, &beta)?, None);
    Ok(())
}

fn retained_bytes_include_compatibility_and_identifier_but_duplicate_copies_cost_nothing<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
            ..TopicConfig::default()
        },
    )?;
    let config = SubscriptionConfig {
        max_message_bytes: MAX_TOPIC_FANOUT_CONTENT_BYTES,
        ..SubscriptionConfig::default()
    };
    let alpha = subscribe(&fixture, "alpha", config, 0)?;
    let beta = subscribe(&fixture, "beta", config, 0)?;
    let mut exact = member("known");
    exact.envelope = MessageEnvelope::default();
    exact.body = vec![
        0;
        MAX_TOPIC_FANOUT_CONTENT_BYTES / 2
            - exact.envelope.content_size()
            - exact.message_id.len()
    ];
    let mut excess = exact.clone();
    excess.body.push(0);
    reject(
        &fixture,
        10,
        rich(excess.clone()),
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ContentBytes,
            maximum: MAX_TOPIC_FANOUT_CONTENT_BYTES,
        },
    )?;
    effects(
        &apply(&fixture, 1, rich(exact.clone()))?,
        &[alpha.clone(), beta.clone()],
    );
    effects(&apply(&fixture, 2, rich(excess))?, &[]);
    for target in [&alpha, &beta] {
        let record = fixture
            .machine
            .message(&fixture.namespace, target, SequenceNumber::new(1))?
            .expect("exact retained copy");
        assert_eq!(record.body.len(), exact.body.len());
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, target, SequenceNumber::new(2))?
                .is_none()
        );
    }
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("accepted duplicate ingress")
            .next_sequence,
        3
    );
    Ok(())
}

fn retained_value_visits_are_multiplied_only_for_nondeduplicated_copies<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    let children = MAX_TOPIC_FANOUT_VALUE_ITEMS / 2 - 1;
    let mut exact = member("array");
    exact.envelope.body =
        MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; children]));
    let mut excess = exact.clone();
    excess.envelope.body =
        MessageBody::Value(MessageValue::Array(vec![MessageValue::Null; children + 1]));
    reject(
        &fixture,
        10,
        rich(excess.clone()),
        BrokerError::TopicFanoutTooLarge {
            limit: IngressBatchLimit::ValueItems,
            maximum: MAX_TOPIC_FANOUT_VALUE_ITEMS,
        },
    )?;
    effects(
        &apply(&fixture, 1, rich(exact))?,
        &[alpha.clone(), beta.clone()],
    );
    effects(&apply(&fixture, 2, rich(excess))?, &[]);
    for target in [&alpha, &beta] {
        let record = fixture
            .machine
            .message(&fixture.namespace, target, SequenceNumber::new(1))?
            .expect("retained array");
        let MessageBody::Value(MessageValue::Array(values)) =
            &record.envelope.expect("typed copy").body
        else {
            panic!("array body")
        };
        assert_eq!(values.len(), children);
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, target, SequenceNumber::new(2))?
                .is_none()
        );
    }
    Ok(())
}

fn input_limits_still_apply_to_zero_subscribers_before_duplicate_or_copy_bounds<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            max_message_bytes: MAX_INGRESS_BATCH_CONTENT_BYTES,
            ..TopicConfig::default()
        },
    )?;
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: vec![member("anonymous"); MAX_INGRESS_BATCH_MESSAGES + 1],
        },
        BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::Messages,
            actual: MAX_INGRESS_BATCH_MESSAGES + 1,
            maximum: MAX_INGRESS_BATCH_MESSAGES,
        },
    )?;
    let id = "large";
    reject(
        &fixture,
        10,
        CommandKind::Send {
            message_id: id.into(),
            body: vec![0; MAX_INGRESS_BATCH_CONTENT_BYTES - id.len() + 1],
            time_to_live_millis: None,
            session_id: None,
        },
        BrokerError::IngressBatchLimitExceeded {
            limit: IngressBatchLimit::ContentBytes,
            actual: MAX_INGRESS_BATCH_CONTENT_BYTES + 1,
            maximum: MAX_INGRESS_BATCH_CONTENT_BYTES,
        },
    )?;
    assert_eq!(counters(&fixture, &fixture.entity)?, None);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    retained_copy_count_has_an_exact_boundary_before_any_commit,
    retained_bytes_include_compatibility_and_identifier_but_duplicate_copies_cost_nothing,
    retained_value_visits_are_multiplied_only_for_nondeduplicated_copies,
    input_limits_still_apply_to_zero_subscribers_before_duplicate_or_copy_bounds,
}
