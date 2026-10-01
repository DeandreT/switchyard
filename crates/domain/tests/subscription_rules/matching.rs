use super::*;

fn all_system_and_custom_conditions_are_anded_and_rules_are_ored_once<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let exact = subscribe(&fixture, "exact", SubscriptionConfig::default(), 0)?;
    let overlap = subscribe(&fixture, "overlap", SubscriptionConfig::default(), 0)?;
    for name in ["exact", "overlap"] {
        remove(&fixture, name, "$Default", 1)?;
    }
    add(
        &fixture,
        "exact",
        "all-fields",
        RuleFilter::Correlation(CorrelationFilter {
            correlation_id: Some("correlation".into()),
            message_id: Some("id".into()),
            to: Some("destination".into()),
            reply_to: Some("reply".into()),
            subject: Some("subject".into()),
            session_id: Some("cart".into()),
            reply_to_session_id: Some("reply-session".into()),
            content_type: Some("application/octet-stream".into()),
            properties: BTreeMap::from([
                ("color".into(), MessageValue::String("Red".into())),
                ("number".into(), MessageValue::Int(7)),
            ]),
        }),
        2,
    )?;
    add(
        &fixture,
        "overlap",
        "subject",
        RuleFilter::Correlation(CorrelationFilter {
            subject: Some("subject".into()),
            ..CorrelationFilter::default()
        }),
        2,
    )?;
    add(
        &fixture,
        "overlap",
        "color",
        correlation([("color".into(), MessageValue::String("Red".into()))]),
        2,
    )?;
    let mut original = member("id");
    original.session_id = Some(SessionId::new("cart")?);
    let mut messages = vec![original.clone()];
    for field in 0..11 {
        let mut changed = original.clone();
        match field {
            0 => {
                changed.envelope.properties.correlation_id =
                    Some(MessageIdentifier::String("Correlation".into()))
            }
            1 => {
                changed.message_id = "different".into();
                changed.envelope.properties.message_id =
                    Some(MessageIdentifier::String("different".into()));
            }
            2 => changed.envelope.properties.to = None,
            3 => changed.envelope.properties.reply_to = Some("Reply".into()),
            4 => changed.envelope.properties.subject = Some("Subject".into()),
            5 => changed.session_id = None,
            6 => changed.envelope.properties.reply_to_group_id = None,
            7 => changed.envelope.properties.content_type = Some("Application/octet-stream".into()),
            8 => {
                changed
                    .envelope
                    .application_properties
                    .insert("color".into(), MessageValue::String("red".into()));
            }
            9 => {
                changed
                    .envelope
                    .application_properties
                    .insert("number".into(), MessageValue::Long(7));
            }
            10 => {
                changed.envelope.application_properties.remove("number");
            }
            _ => unreachable!(),
        }
        messages.push(changed);
    }
    let application = publish(&fixture, 3, messages)?;
    effects(&application, &[exact.clone(), overlap.clone()]);
    assert_eq!(peek(&fixture, &exact, 3)?.len(), 1);
    let retained = record(&fixture, &exact, 1)?.expect("all conditions matched");
    assert_eq!(retained.envelope.as_deref(), Some(&original.envelope));
    assert_eq!(retained.session_id, Some(SessionId::new("cart")?));
    assert_eq!(peek(&fixture, &overlap, 3)?.len(), 12);
    for sequence in 2..=12 {
        assert!(record(&fixture, &exact, sequence)?.is_none());
    }
    assert_eq!(counters(&fixture, &exact)?, None);
    assert_eq!(counters(&fixture, &overlap)?, None);
    Ok(())
}

fn scalar_equality_is_exact_and_present_null_is_not_a_missing_property<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "typed", SubscriptionConfig::default(), 0)?;
    remove(&fixture, "typed", "$Default", 1)?;
    let values = vec![
        MessageValue::Null,
        MessageValue::Bool(true),
        MessageValue::Ubyte(7),
        MessageValue::Ushort(7),
        MessageValue::Uint(7),
        MessageValue::Ulong(7),
        MessageValue::Byte(7),
        MessageValue::Short(7),
        MessageValue::Int(7),
        MessageValue::Long(7),
        MessageValue::Float(0x8000_0000),
        MessageValue::Double(0x7ff8_0000_0000_0001),
        MessageValue::Decimal32([1; 4]),
        MessageValue::Decimal64([2; 8]),
        MessageValue::Decimal128([3; 16]),
        MessageValue::Char('a'),
        MessageValue::Timestamp(7),
        MessageValue::Uuid([4; 16]),
        MessageValue::Binary(vec![0, 255]),
        MessageValue::String("Red".into()),
        MessageValue::Symbol("symbol".into()),
    ];
    for (index, value) in values.into_iter().enumerate() {
        let millis = 2 + index as u64 * 3;
        add(
            &fixture,
            "typed",
            "value",
            correlation([("value".into(), value.clone())]),
            millis,
        )?;
        let mut matches = member("scalar");
        matches
            .envelope
            .application_properties
            .insert("value".into(), value.clone());
        let mut wrong = matches.clone();
        wrong.envelope.application_properties.insert(
            "value".into(),
            match value {
                MessageValue::Null => MessageValue::Bool(false),
                MessageValue::Int(7) => MessageValue::Long(7),
                MessageValue::Float(_) => MessageValue::Float(0),
                MessageValue::Double(_) => MessageValue::Double(0x7ff8_0000_0000_0002),
                MessageValue::String(_) => MessageValue::String("red".into()),
                _ => MessageValue::Null,
            },
        );
        let mut missing = matches.clone();
        missing.envelope.application_properties.remove("value");
        effects(
            &publish(&fixture, millis + 1, vec![matches.clone(), wrong, missing])?,
            std::slice::from_ref(&child),
        );
        let sequence = index as u64 * 3 + 1;
        assert_eq!(
            record(&fixture, &child, sequence)?
                .expect("exact scalar")
                .envelope
                .as_deref(),
            Some(&matches.envelope)
        );
        assert!(record(&fixture, &child, sequence + 1)?.is_none());
        assert!(record(&fixture, &child, sequence + 2)?.is_none());
        remove(&fixture, "typed", "value", millis + 2)?;
    }
    assert_eq!(peek(&fixture, &child, 100)?.len(), 21);
    add(
        &fixture,
        "typed",
        "case-key",
        correlation([("COLOR".into(), MessageValue::String("Red".into()))]),
        100,
    )?;
    effects(
        &publish(&fixture, 101, vec![member("exact-property-name")])?,
        &[],
    );
    Ok(())
}

fn normalized_message_ids_and_raw_identifier_types_keep_their_original_authority<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let child = subscribe(&fixture, "ids", SubscriptionConfig::default(), 0)?;
    remove(&fixture, "ids", "$Default", 1)?;
    for (index, (normalized, raw)) in [
        (
            "literal".to_string(),
            MessageIdentifier::String("literal".into()),
        ),
        ("7".into(), MessageIdentifier::Ulong(7)),
        (
            format!("Uuid({:?})", [7u8; 16]),
            MessageIdentifier::Uuid([7; 16]),
        ),
        ("00ff".into(), MessageIdentifier::Binary(vec![0, 255])),
    ]
    .into_iter()
    .enumerate()
    {
        let millis = 2 + index as u64 * 3;
        add(
            &fixture,
            "ids",
            "id",
            RuleFilter::Correlation(CorrelationFilter {
                message_id: Some(normalized.clone()),
                ..CorrelationFilter::default()
            }),
            millis,
        )?;
        let mut message = member(&normalized);
        message.envelope.properties.message_id = Some(raw.clone());
        effects(
            &publish(&fixture, millis + 1, vec![message])?,
            std::slice::from_ref(&child),
        );
        let stored = record(&fixture, &child, index as u64 + 1)?.expect("normalized ID match");
        assert_eq!(stored.message_id, normalized);
        assert_eq!(
            stored
                .envelope
                .expect("raw properties retained")
                .properties
                .message_id,
            Some(raw)
        );
        remove(&fixture, "ids", "id", millis + 2)?;
    }
    add(
        &fixture,
        "ids",
        "correlation",
        RuleFilter::Correlation(CorrelationFilter {
            correlation_id: Some("7".into()),
            ..CorrelationFilter::default()
        }),
        20,
    )?;
    let mut string = member("string-correlation");
    string.envelope.properties.correlation_id = Some(MessageIdentifier::String("7".into()));
    let mut numeric = member("numeric-correlation");
    numeric.envelope.properties.correlation_id = Some(MessageIdentifier::Ulong(7));
    effects(
        &publish(&fixture, 21, vec![string, numeric])?,
        std::slice::from_ref(&child),
    );
    assert!(record(&fixture, &child, 5)?.is_some());
    assert!(record(&fixture, &child, 6)?.is_none());
    Ok(())
}

fn filtering_precedes_session_dlq_projection_and_never_modifies_other_copies<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, TopicConfig::default())?;
    let ordinary = subscribe(&fixture, "plain", SubscriptionConfig::default(), 0)?;
    let required = subscribe(
        &fixture,
        "session",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    remove(&fixture, "session", "$Default", 1)?;
    add(
        &fixture,
        "session",
        "red",
        correlation([("color".into(), MessageValue::String("Red".into()))]),
        2,
    )?;
    let mut excluded = member("excluded-null");
    excluded
        .envelope
        .application_properties
        .insert("color".into(), MessageValue::String("Blue".into()));
    effects(
        &publish(&fixture, 3, vec![excluded])?,
        std::slice::from_ref(&ordinary),
    );
    let shadow = required.dead_letter_queue()?;
    assert!(peek(&fixture, &shadow, 3)?.is_empty());
    let included = member("included-null");
    effects(
        &publish(&fixture, 4, vec![included.clone()])?,
        &[ordinary.clone(), shadow.clone()],
    );
    let letter = record(&fixture, &shadow, 2)?.expect("matched null-session copy");
    assert_eq!(
        letter.dead_letter.expect("canonical failure").reason,
        DeadLetterReason::MissingSessionId
    );
    assert_eq!(letter.envelope.as_deref(), Some(&included.envelope));
    assert!(record(&fixture, &required, 2)?.is_none());
    let mut named = member("included-named");
    named.session_id = Some(SessionId::new("cart")?);
    effects(
        &publish(&fixture, 5, vec![named.clone()])?,
        &[ordinary.clone(), required.clone()],
    );
    for entity in [&ordinary, &required] {
        let stored = record(&fixture, entity, 3)?.expect("independent matched copy");
        assert_eq!(stored.session_id, named.session_id);
        assert_eq!(stored.envelope.as_deref(), Some(&named.envelope));
    }
    assert!(record(&fixture, &shadow, 3)?.is_none());
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    all_system_and_custom_conditions_are_anded_and_rules_are_ored_once,
    scalar_equality_is_exact_and_present_null_is_not_a_missing_property,
    normalized_message_ids_and_raw_identifier_types_keep_their_original_authority,
    filtering_precedes_session_dlq_projection_and_never_modifies_other_copies,
}
