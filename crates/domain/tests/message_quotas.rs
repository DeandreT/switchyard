//! Local conservative property/header quotas are enforced before enqueue.

use std::{collections::BTreeMap, error::Error};

use domain::{
    AnnotationKey, BrokerError, CommandKind, CommandOutcome, MAX_MESSAGE_HEADER_BYTES,
    MAX_MESSAGE_PROPERTY_BYTES, MessageBody, MessageEnvelope, MessageIdentifier, MessageProperties,
    MessageValue, QueueConfig, ReceiveMode, ScheduledEnvelope, SequenceNumber, Timestamp,
};
use storage::StateStore;
use testkit::{QueueFixture, StoreProvider};

fn binary_property(key: &str, property_bytes: usize) -> MessageEnvelope {
    let payload_bytes = property_bytes - 10 - key.len();
    MessageEnvelope {
        application_properties: BTreeMap::from([(
            key.to_owned(),
            MessageValue::Binary(vec![0; payload_bytes]),
        )]),
        ..MessageEnvelope::default()
    }
}

fn cumulative_header(header_bytes: usize) -> MessageEnvelope {
    let mut envelope = binary_property("first", MAX_MESSAGE_PROPERTY_BYTES);
    envelope
        .application_properties
        .insert(String::from("second"), MessageValue::Binary(Vec::new()));
    let remaining = header_bytes - envelope.header_content_size();
    envelope.application_properties.insert(
        String::from("second"),
        MessageValue::Binary(vec![0; remaining]),
    );
    envelope
}

#[test]
fn application_property_boundary_includes_its_key_and_typed_value_overhead() {
    let envelope = binary_property("payload", MAX_MESSAGE_PROPERTY_BYTES);
    assert_eq!(envelope.validate(), Ok(()));
    assert_eq!(
        binary_property("payload", MAX_MESSAGE_PROPERTY_BYTES + 1).validate(),
        Err(BrokerError::MessagePropertyTooLarge {
            property: String::from("payload"),
            property_bytes: MAX_MESSAGE_PROPERTY_BYTES + 1,
            maximum_bytes: MAX_MESSAGE_PROPERTY_BYTES,
        })
    );
}

#[test]
fn utf8_property_keys_and_string_values_are_measured_in_bytes() {
    let key = String::from("\u{e9}");
    let mut value = "\u{1f600}".repeat((MAX_MESSAGE_PROPERTY_BYTES - key.len() - 10) / 4);
    assert_eq!(key.chars().count(), 1);
    assert_eq!(key.len(), 2);
    let mut envelope = MessageEnvelope {
        application_properties: BTreeMap::from([(
            key.clone(),
            MessageValue::String(value.clone()),
        )]),
        ..MessageEnvelope::default()
    };
    assert_eq!(envelope.validate(), Ok(()));
    value.push('x');
    envelope
        .application_properties
        .insert(key.clone(), MessageValue::String(value));
    assert_eq!(
        envelope.validate(),
        Err(BrokerError::MessagePropertyTooLarge {
            property: key,
            property_bytes: MAX_MESSAGE_PROPERTY_BYTES + 1,
            maximum_bytes: MAX_MESSAGE_PROPERTY_BYTES,
        })
    );
}

#[test]
fn standard_properties_use_named_individual_checks_but_positional_header_accounting() {
    let named_size = |name: &str| MAX_MESSAGE_PROPERTY_BYTES - name.len() - 10;
    let mut envelope = MessageEnvelope {
        properties: MessageProperties {
            subject: Some("x".repeat(named_size("subject"))),
            ..MessageProperties::default()
        },
        ..MessageEnvelope::default()
    };
    assert_eq!(envelope.validate(), Ok(()));
    let baseline = MessageEnvelope::default().header_content_size();
    assert_eq!(
        envelope.header_content_size(),
        baseline - 1 + 5 + named_size("subject")
    );
    envelope
        .properties
        .subject
        .as_mut()
        .expect("subject")
        .push('x');
    assert_eq!(
        envelope.validate(),
        Err(BrokerError::MessagePropertyTooLarge {
            property: String::from("subject"),
            property_bytes: MAX_MESSAGE_PROPERTY_BYTES + 1,
            maximum_bytes: MAX_MESSAGE_PROPERTY_BYTES,
        })
    );

    for (name, properties) in [
        (
            "user-id",
            MessageProperties {
                user_id: Some(vec![0; named_size("user-id") + 1]),
                ..MessageProperties::default()
            },
        ),
        (
            "correlation-id",
            MessageProperties {
                correlation_id: Some(MessageIdentifier::Binary(vec![
                    0;
                    named_size("correlation-id")
                        + 1
                ])),
                ..MessageProperties::default()
            },
        ),
        (
            "reply-to-group-id",
            MessageProperties {
                reply_to_group_id: Some("x".repeat(named_size("reply-to-group-id") + 1)),
                ..MessageProperties::default()
            },
        ),
    ] {
        assert_eq!(
            MessageEnvelope {
                properties,
                ..MessageEnvelope::default()
            }
            .validate(),
            Err(BrokerError::MessagePropertyTooLarge {
                property: name.to_owned(),
                property_bytes: MAX_MESSAGE_PROPERTY_BYTES + 1,
                maximum_bytes: MAX_MESSAGE_PROPERTY_BYTES,
            })
        );
    }
}

#[test]
fn both_annotation_key_types_use_their_real_conservative_key_sizes() {
    for (key, name, key_bytes) in [
        (AnnotationKey::Symbol(String::from("custom")), "custom", 11),
        (AnnotationKey::Ulong(7), "annotation[7]", 9),
    ] {
        let mut envelope = MessageEnvelope {
            message_annotations: BTreeMap::from([(
                key.clone(),
                MessageValue::Binary(vec![0; MAX_MESSAGE_PROPERTY_BYTES - key_bytes - 5]),
            )]),
            ..MessageEnvelope::default()
        };
        assert_eq!(envelope.validate(), Ok(()));
        envelope.message_annotations.insert(
            key,
            MessageValue::Binary(vec![0; MAX_MESSAGE_PROPERTY_BYTES - key_bytes - 4]),
        );
        assert_eq!(
            envelope.validate(),
            Err(BrokerError::MessagePropertyTooLarge {
                property: name.to_owned(),
                property_bytes: MAX_MESSAGE_PROPERTY_BYTES + 1,
                maximum_bytes: MAX_MESSAGE_PROPERTY_BYTES,
            })
        );
    }
}

#[test]
fn individually_valid_properties_share_the_cumulative_header_limit() {
    let envelope = cumulative_header(MAX_MESSAGE_HEADER_BYTES);
    assert_eq!(envelope.header_content_size(), MAX_MESSAGE_HEADER_BYTES);
    assert_eq!(envelope.validate(), Ok(()));
    let envelope = cumulative_header(MAX_MESSAGE_HEADER_BYTES + 1);
    assert_eq!(
        envelope.validate(),
        Err(BrokerError::MessageHeaderTooLarge {
            header_bytes: MAX_MESSAGE_HEADER_BYTES + 1,
            maximum_bytes: MAX_MESSAGE_HEADER_BYTES,
        })
    );
}

#[test]
fn standard_application_and_annotation_properties_share_one_header_quota() {
    let mut envelope = MessageEnvelope {
        properties: MessageProperties {
            subject: Some("x".repeat(20_000)),
            ..MessageProperties::default()
        },
        application_properties: BTreeMap::from([(
            String::from("app"),
            MessageValue::Binary(vec![0; 20_000]),
        )]),
        message_annotations: BTreeMap::from([(
            AnnotationKey::Symbol(String::from("annotation")),
            MessageValue::Binary(vec![0; 20_000]),
        )]),
        ..MessageEnvelope::default()
    };
    assert_eq!(envelope.validate(), Ok(()));
    envelope.message_annotations.insert(
        AnnotationKey::Symbol(String::from("annotation")),
        MessageValue::Binary(vec![0; 28_000]),
    );
    assert_eq!(
        envelope.validate(),
        Err(BrokerError::MessageHeaderTooLarge {
            header_bytes: envelope.header_content_size(),
            maximum_bytes: MAX_MESSAGE_HEADER_BYTES,
        })
    );
}

#[test]
fn body_and_footer_do_not_spend_header_or_individual_property_quota() {
    let mut envelope = cumulative_header(MAX_MESSAGE_HEADER_BYTES);
    let header_bytes = envelope.header_content_size();
    let content_bytes = envelope.content_size();
    envelope.body = MessageBody::Value(MessageValue::String("x".repeat(MAX_MESSAGE_HEADER_BYTES)));
    envelope.footer.insert(
        AnnotationKey::Symbol(String::from("checksum")),
        MessageValue::Binary(vec![0; MAX_MESSAGE_HEADER_BYTES + 1]),
    );
    assert_eq!(envelope.header_content_size(), header_bytes);
    assert!(envelope.content_size() > content_bytes + 2 * MAX_MESSAGE_HEADER_BYTES);
    assert_eq!(envelope.validate(), Ok(()));
}

#[test]
fn shape_validation_precedes_size_quota_validation() {
    let envelope = MessageEnvelope {
        application_properties: BTreeMap::from([(
            String::from("compound"),
            MessageValue::List(vec![MessageValue::Binary(vec![
                0;
                MAX_MESSAGE_PROPERTY_BYTES
            ])]),
        )]),
        ..MessageEnvelope::default()
    };
    assert!(matches!(
        envelope.validate(),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    envelope: MessageEnvelope,
) -> Result<SequenceNumber, BrokerError> {
    match fixture.at(
        millis,
        CommandKind::SendEnvelope {
            message_id: id.to_owned(),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
            envelope: Box::new(envelope),
        },
    )? {
        CommandOutcome::Sent { sequence } => Ok(sequence),
        other => panic!("expected sent outcome, got {other:?}"),
    }
}

fn duplicate_fixture<P: StoreProvider>(provider: P) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?)
}

fn property_rejection_is_atomic_before_duplicate_lookup<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = duplicate_fixture(provider)?;
    let oversized = binary_property("large", MAX_MESSAGE_PROPERTY_BYTES + 1);
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert!(matches!(
        send(&fixture, 10, "same", oversized.clone()),
        Err(BrokerError::MessagePropertyTooLarge { .. })
    ));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    let sequence = send(&fixture, 11, "same", MessageEnvelope::default())?;
    assert_eq!(sequence, SequenceNumber::new(1));
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert!(matches!(
        send(&fixture, 12, "same", oversized),
        Err(BrokerError::MessagePropertyTooLarge { .. })
    ));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?
            .is_some()
    );
    Ok(())
}

fn header_rejection_is_atomic_before_duplicate_lookup<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = duplicate_fixture(provider)?;
    let oversized = cumulative_header(MAX_MESSAGE_HEADER_BYTES + 1);
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert!(matches!(
        send(&fixture, 10, "same", oversized.clone()),
        Err(BrokerError::MessageHeaderTooLarge { .. })
    ));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    assert_eq!(
        send(&fixture, 11, "same", MessageEnvelope::default())?,
        SequenceNumber::new(1)
    );
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert!(matches!(
        send(&fixture, 12, "same", oversized),
        Err(BrokerError::MessageHeaderTooLarge { .. })
    ));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    Ok(())
}

fn scheduled_quota_rejections_leave_the_entire_batch_and_duplicate_index_unchanged<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = duplicate_fixture(provider)?;
    let make = |id: &str, envelope| ScheduledEnvelope {
        message_id: id.to_owned(),
        body: Vec::new(),
        time_to_live_millis: None,
        session_id: None,
        enqueue_at: Timestamp::from_millis(100),
        envelope,
    };
    for (millis, invalid) in [
        (10, binary_property("large", MAX_MESSAGE_PROPERTY_BYTES + 1)),
        (11, cumulative_header(MAX_MESSAGE_HEADER_BYTES + 1)),
    ] {
        let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
        assert!(matches!(
            fixture.at(
                millis,
                CommandKind::ScheduleEnvelopes {
                    messages: vec![
                        make("first", MessageEnvelope::default()),
                        make("second", invalid),
                    ],
                }
            ),
            Err(BrokerError::MessagePropertyTooLarge { .. }
                | BrokerError::MessageHeaderTooLarge { .. })
        ));
        assert_eq!(
            fixture.machine.store().scan_prefix(&[], usize::MAX)?,
            before
        );
    }
    assert_eq!(
        send(&fixture, 12, "first", MessageEnvelope::default())?,
        SequenceNumber::new(1)
    );
    assert_eq!(
        send(&fixture, 13, "second", MessageEnvelope::default())?,
        SequenceNumber::new(2)
    );
    Ok(())
}

fn large_footer_uses_only_the_total_message_limit_and_survives_storage<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let envelope = MessageEnvelope {
        footer: BTreeMap::from([(
            AnnotationKey::Ulong(7),
            MessageValue::Binary(vec![0; MAX_MESSAGE_HEADER_BYTES + 1]),
        )]),
        ..MessageEnvelope::default()
    };
    send(&fixture, 10, "footer", envelope.clone())?;
    let fixture = fixture.restart()?;
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        11,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("stored footer message should be ready");
    };
    assert_eq!(delivery.envelope.as_deref(), Some(&envelope));
    let oversized = MessageEnvelope {
        footer: BTreeMap::from([(
            AnnotationKey::Ulong(7),
            MessageValue::Binary(vec![0; domain::DEFAULT_MAX_MESSAGE_BYTES]),
        )]),
        ..MessageEnvelope::default()
    };
    let before = fixture.machine.store().scan_prefix(&[], usize::MAX)?;
    assert!(matches!(
        send(&fixture, 12, "oversized-footer", oversized),
        Err(BrokerError::MessageTooLarge { .. })
    ));
    assert_eq!(
        fixture.machine.store().scan_prefix(&[], usize::MAX)?,
        before
    );
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    property_rejection_is_atomic_before_duplicate_lookup,
    header_rejection_is_atomic_before_duplicate_lookup,
    scheduled_quota_rejections_leave_the_entire_batch_and_duplicate_index_unchanged,
    large_footer_uses_only_the_total_message_limit_and_survives_storage,
}
