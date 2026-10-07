use super::*;
use crate::{
    DeadLetterInfo, DeadLetterReason, MessageBody, MessageIdentifier, MessageProperties,
    MessageState, MessageValue, SequenceNumber, Timestamp,
};

fn record() -> MessageRecord {
    MessageRecord {
        sequence: SequenceNumber::new(1),
        message_id: "id".into(),
        body: vec![1, 2, 3],
        enqueued_at: Timestamp::from_millis(1),
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: None,
        scheduled_enqueue_time: None,
        envelope: None,
    }
}

fn finite(limit: u64) -> QueueCapacityMode {
    QueueCapacityMode::finite_v1(1, NonZeroU64::new(limit).unwrap()).unwrap()
}

fn dead_letter(
    mut record: MessageRecord,
    reason: DeadLetterReason,
    description: &str,
) -> MessageRecord {
    record.state = MessageState::Ready;
    record.session_id = None;
    record.expires_at = None;
    record.dead_letter = Some(DeadLetterInfo {
        reason,
        description: description.into(),
        dead_lettered_at: Timestamp::from_millis(2),
    });
    record
}

fn raw_usage(schema: u8, generation: u64, model: u8, bytes: u64, count: u64) -> Vec<u8> {
    codec::encode(&(schema, generation, model, bytes, count)).unwrap()
}

fn raw_charge(
    schema: u8,
    generation: u64,
    model: u8,
    producer: u64,
    session: u64,
    dead_letter: u64,
    charged: u64,
) -> Vec<u8> {
    codec::encode(&(
        schema,
        generation,
        model,
        producer,
        session,
        dead_letter,
        charged,
    ))
    .unwrap()
}

#[test]
fn legacy_charge_counts_utf8_identifier_and_session_bytes() {
    let mut record = record();
    record.message_id = "\u{e9}".into();
    record.session_id = Some(SessionId::new("\u{e9}").unwrap());
    let charge = MessageCharge::for_new_record(1, &record).unwrap();
    assert_eq!(charge.producer_bytes(), 3 + 5 + 2);
    assert_eq!(charge.original_session_bytes(), 5 + 2);
    assert_eq!(charge.dead_letter_projection_bytes(), 0);
    assert_eq!(charge.charged_bytes(), 10 + 7 + 256 + 256);
    charge.validate_record(&record).unwrap();
}

#[test]
fn rich_charge_counts_missing_identifier_but_not_the_body_twice() {
    let mut record = record();
    let envelope = MessageEnvelope {
        body: MessageBody::Data(vec![record.body.clone()]),
        ..MessageEnvelope::default()
    };
    let content = u64::try_from(envelope.content_size()).unwrap();
    record.envelope = Some(Box::new(envelope));
    let missing = MessageCharge::for_new_record(1, &record).unwrap();
    assert_eq!(missing.producer_bytes(), content + 5 + 2);
    let mut envelope = record.envelope.take().unwrap();
    envelope.properties.message_id = Some(MessageIdentifier::String("id".into()));
    let content = u64::try_from(envelope.content_size()).unwrap();
    record.envelope = Some(envelope);
    let authoritative = MessageCharge::for_new_record(1, &record).unwrap();
    assert_eq!(authoritative.producer_bytes(), content);
    record.body = vec![0; 1_000];
    let compatibility = MessageCharge::for_new_record(1, &record).unwrap();
    assert_eq!(compatibility.producer_bytes(), 1_000);
    assert_ne!(compatibility.producer_bytes(), 1_000 + content);
    record.envelope.as_mut().unwrap().properties.message_id = Some(MessageIdentifier::Ulong(42));
    record.body.clear();
    let typed_identifier = MessageCharge::for_new_record(1, &record).unwrap();
    assert_eq!(
        typed_identifier.producer_bytes(),
        u64::try_from(record.envelope.as_ref().unwrap().content_size()).unwrap()
    );
}

#[test]
fn automatic_dead_letters_preserve_original_credit_at_full_capacity() {
    for (reason, description, projected) in [
        (
            DeadLetterReason::TimeToLiveExpired,
            "the message exceeded its time to live",
            137,
        ),
        (
            DeadLetterReason::MaxDeliveryCountExceeded,
            "the message reached its maximum delivery count",
            151,
        ),
    ] {
        let mut original = record();
        original.session_id = Some(SessionId::new("session").unwrap());
        let charge = MessageCharge::for_new_record(1, &original).unwrap();
        let mode = finite(charge.charged_bytes());
        let full = QueueCapacityUsage::new(1, charge.charged_bytes(), 1).unwrap();
        let shadow = dead_letter(original, reason, description);
        let proposed = charge.recharge_retained_record(&shadow).unwrap();
        let observed = observe_record(&shadow).unwrap();
        assert!(observed.is_dead_letter());
        assert_eq!(charge.recharge_observation(observed), Ok(proposed));
        assert_eq!(proposed.dead_letter_projection_bytes(), projected);
        assert_eq!(proposed.original_session_bytes(), 12);
        assert_eq!(proposed.charged_bytes(), charge.charged_bytes());
        proposed.validate_record(&shadow).unwrap();
        assert_eq!(full.replace(mode, charge, proposed), Ok(full));
        assert_eq!(
            MessageCharge::for_new_record(1, &shadow),
            Err(QueueCapacityError::OriginalSessionRequired)
        );
    }
}

#[test]
fn explicit_utf8_dead_letter_growth_acquires_credit_and_shrink_returns_it() {
    let original = record();
    let charge = MessageCharge::for_new_record(1, &original).unwrap();
    let full = QueueCapacityUsage::new(1, charge.charged_bytes(), 1).unwrap();
    let shadow = dead_letter(
        original,
        DeadLetterReason::Application("\u{e9}".repeat(100)),
        "x",
    );
    let proposed = charge.recharge_retained_record(&shadow).unwrap();
    assert_eq!(proposed.dead_letter_projection_bytes(), 81 + 200 + 1);
    assert_eq!(proposed.charged_bytes(), charge.charged_bytes() + 26);
    assert_eq!(
        full.replace(finite(full.reserved_bytes()), charge, proposed),
        Err(QueueCapacityError::LimitExceeded)
    );
    assert_eq!(full.reserved_bytes(), charge.charged_bytes());
    let grown = full
        .replace(finite(proposed.charged_bytes()), charge, proposed)
        .unwrap();
    assert_eq!(grown.message_count(), 1);
    assert_eq!(grown.reserved_bytes(), proposed.charged_bytes());
    let mut smaller = shadow;
    smaller.dead_letter.as_mut().unwrap().reason = DeadLetterReason::Application("\u{e9}".into());
    smaller.dead_letter.as_mut().unwrap().description = "\u{1f600}".into();
    let shrunk = proposed.recharge_retained_record(&smaller).unwrap();
    assert_eq!(shrunk.dead_letter_projection_bytes(), 81 + 2 + 4);
    assert_eq!(shrunk.charged_bytes(), charge.charged_bytes());
    assert_eq!(
        grown.replace(finite(proposed.charged_bytes()), proposed, shrunk),
        Ok(full)
    );
}

#[test]
fn retained_rich_property_growth_and_shrink_recompute_only_producer_credit() {
    let mut original = record();
    original.envelope = Some(Box::new(MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String("id".into())),
            ..MessageProperties::default()
        },
        ..MessageEnvelope::default()
    }));
    original
        .envelope
        .as_mut()
        .unwrap()
        .application_properties
        .insert("label".into(), MessageValue::String("\u{e9}".into()));
    let charge = MessageCharge::for_new_record(1, &original).unwrap();
    let mut larger = original.clone();
    larger
        .envelope
        .as_mut()
        .unwrap()
        .application_properties
        .insert("label".into(), MessageValue::String("\u{e9}".repeat(100)));
    let grown = charge.recharge_retained_record(&larger).unwrap();
    assert_eq!(grown.producer_bytes(), charge.producer_bytes() + 198);
    assert_eq!(grown.charged_bytes(), charge.charged_bytes() + 198);
    assert_eq!(grown.dead_letter_projection_bytes(), 0);
    assert_eq!(
        charge.validate_record(&larger),
        Err(QueueCapacityError::RecordMismatch)
    );
    assert_eq!(grown.recharge_retained_record(&original), Ok(charge));
}

#[test]
fn original_numeric_observation_survives_mutation_and_refund_uses_old_ledger() {
    let mut record = record();
    record.envelope = Some(Box::new(MessageEnvelope {
        properties: MessageProperties {
            message_id: Some(MessageIdentifier::String("id".into())),
            ..MessageProperties::default()
        },
        ..MessageEnvelope::default()
    }));
    let charge = MessageCharge::for_new_record(1, &record).unwrap();
    let original = observe_record(&record);
    assert!(!original.as_ref().unwrap().is_dead_letter());
    record
        .envelope
        .as_mut()
        .unwrap()
        .application_properties
        .insert("label".into(), MessageValue::String("\u{e9}".repeat(100)));
    // The mutation's normal validations finish before original errors propagate.
    record.envelope.as_ref().unwrap().validate().unwrap();
    charge.validate_observation(original.unwrap()).unwrap();
    assert_eq!(
        charge.validate_record(&record),
        Err(QueueCapacityError::RecordMismatch)
    );
    let proposed = charge
        .recharge_observation(observe_record(&record).unwrap())
        .unwrap();
    assert!(proposed.charged_bytes() > charge.charged_bytes());
    let full = QueueCapacityUsage::new(1, charge.charged_bytes(), 1).unwrap();
    assert_eq!(
        full.refund(finite(charge.charged_bytes()), charge),
        Ok(QueueCapacityUsage::new(1, 0, 0).unwrap())
    );
    assert_eq!(
        full.refund(finite(charge.charged_bytes()), proposed),
        Err(QueueCapacityError::ArithmeticUnderflow)
    );
}

#[test]
fn charge_is_unchanged_by_lock_state_and_schedule_metadata() {
    let original = record();
    let charge = MessageCharge::for_new_record(1, &original).unwrap();
    for state in [
        MessageState::Deferred,
        MessageState::Locked {
            token: crate::LockToken::new(2),
            locked_until: Timestamp::from_millis(99),
        },
        MessageState::Scheduled {
            enqueue_at: Timestamp::from_millis(50),
            time_to_live_millis: Some(10),
        },
    ] {
        let mut changed = original.clone();
        changed.state = state;
        changed.delivery_count = u32::MAX;
        changed.scheduled_enqueue_time = Some(Timestamp::from_millis(50));
        assert_eq!(charge.recharge_retained_record(&changed), Ok(charge));
    }
}

#[test]
fn original_session_reserve_is_retained_and_live_session_mismatches_are_refused() {
    let mut original = record();
    original.session_id = Some(SessionId::new("s".repeat(MAX_SESSION_ID_BYTES)).unwrap());
    let charge = MessageCharge::for_new_record(1, &original).unwrap();
    assert_eq!(charge.original_session_bytes(), 133);
    let shadow = dead_letter(
        original.clone(),
        DeadLetterReason::TimeToLiveExpired,
        "the message exceeded its time to live",
    );
    let retained = charge.recharge_retained_record(&shadow).unwrap();
    assert_eq!(retained.original_session_bytes(), 133);
    assert_eq!(retained.charged_bytes(), charge.charged_bytes());
    original.session_id = None;
    assert_eq!(
        charge.validate_record(&original),
        Err(QueueCapacityError::InvalidSessionMetadata)
    );
    let mut malformed_shadow = shadow;
    malformed_shadow.session_id = Some(SessionId::new("s").unwrap());
    assert_eq!(
        retained.validate_record(&malformed_shadow),
        Err(QueueCapacityError::InvalidSessionMetadata)
    );
    for raw in ["", "s\0", &"s".repeat(MAX_SESSION_ID_BYTES + 1)] {
        let session: SessionId = codec::decode(&codec::encode(&raw).unwrap()).unwrap();
        assert_eq!(
            session_bytes(Some(&session)),
            Err(QueueCapacityError::InvalidSessionMetadata)
        );
    }
}

#[test]
fn usage_reserve_refund_and_replace_are_checked_immutable_values() {
    let charge = MessageCharge::for_new_record(1, &record()).unwrap();
    let mode = finite(charge.charged_bytes());
    let empty = QueueCapacityUsage::new(1, 0, 0).unwrap();
    let full = empty.reserve(mode, charge).unwrap();
    assert_eq!(empty.reserved_bytes(), 0);
    assert_eq!(full.message_count(), 1);
    assert_eq!(
        full.reserve(mode, charge),
        Err(QueueCapacityError::LimitExceeded)
    );
    assert_eq!(full.refund(mode, charge), Ok(empty));
    assert_eq!(
        empty.refund(mode, charge),
        Err(QueueCapacityError::ArithmeticUnderflow)
    );
    assert_eq!(
        empty.replace(mode, charge, charge),
        Err(QueueCapacityError::ArithmeticUnderflow)
    );
    assert_eq!(
        full,
        QueueCapacityUsage::new(1, charge.charged_bytes(), 1).unwrap()
    );
    assert_eq!(
        empty.validate_mode(QueueCapacityMode::non_finite(1).unwrap()),
        Err(QueueCapacityError::UnexpectedUsage)
    );
    let other = MessageCharge::for_new_record(2, &record()).unwrap();
    assert_eq!(
        full.refund(mode, other),
        Err(QueueCapacityError::GenerationMismatch)
    );
}

#[test]
fn finite_profile_refuses_unaccounted_features_without_changing_non_finite_behavior() {
    for config in [
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    ] {
        assert_eq!(
            finite(1).validate_config(&config),
            Err(QueueCapacityError::UnsupportedFiniteQueue)
        );
        QueueCapacityMode::non_finite(1)
            .unwrap()
            .validate_config(&config)
            .unwrap();
    }
    finite(1).validate_config(&QueueConfig::default()).unwrap();
    assert_eq!(finite(1).limit_bytes().unwrap().get(), 1);
    let mut optional_session = record();
    optional_session.session_id = Some(SessionId::new("optional").unwrap());
    assert!(MessageCharge::for_new_record(1, &optional_session).is_ok());
}

#[test]
fn raw_stored_usage_above_limit_is_corruption_not_proposed_quota_exhaustion() {
    let raw = raw_usage(1, 1, 1, 518, 1);
    let stored = QueueCapacityUsage::decode(&raw, 1).unwrap();
    let mode = finite(517);
    let charge = MessageCharge::new(1, ChargeComponents::new(5, 0, 0).unwrap()).unwrap();
    assert_eq!(
        stored.validate_mode(mode),
        Err(QueueCapacityError::InvalidUsage)
    );
    assert_eq!(
        stored.reserve(mode, charge),
        Err(QueueCapacityError::InvalidUsage)
    );
    assert_eq!(
        stored.replace(mode, charge, charge),
        Err(QueueCapacityError::InvalidUsage)
    );
    assert_eq!(
        stored.refund(mode, charge),
        Err(QueueCapacityError::InvalidUsage)
    );
    assert_eq!(stored.encode().unwrap(), raw);
    let valid = QueueCapacityUsage::new(1, 517, 1).unwrap();
    assert_eq!(
        valid.reserve(mode, charge),
        Err(QueueCapacityError::LimitExceeded)
    );
    assert_eq!(valid.reserved_bytes(), 517);
    assert_eq!(valid.message_count(), 1);
}

#[test]
fn pure_component_boundaries_use_checked_not_saturating_arithmetic() {
    assert_eq!(
        checked_content_tally(usize::MAX),
        Err(QueueCapacityError::SaturatedContentTally)
    );
    assert_eq!(
        ChargeComponents::new(u64::MAX - 512, 0, 0)
            .unwrap()
            .charged_bytes,
        u64::MAX
    );
    assert_eq!(
        ChargeComponents::new(u64::MAX - 511, 0, 0),
        Err(QueueCapacityError::ArithmeticOverflow)
    );
    assert_eq!(
        ChargeComponents::new(u64::MAX, 6, 0),
        Err(QueueCapacityError::ArithmeticOverflow)
    );
    assert_eq!(
        ChargeComponents::new(5, 0, u64::MAX),
        Err(QueueCapacityError::ArithmeticOverflow)
    );
    assert_eq!(ChargeComponents::new(5, 6, 81).unwrap().charged_bytes, 523);
    assert_eq!(
        ChargeComponents::new(5, 133, 256).unwrap().charged_bytes,
        650
    );
    for (producer, session, dead_letter) in [(4, 0, 0), (5, 5, 0), (5, 134, 0), (5, 0, 80)] {
        assert_eq!(
            ChargeComponents::new(producer, session, dead_letter),
            Err(QueueCapacityError::InvalidCharge)
        );
    }
    let exact =
        MessageCharge::new(1, ChargeComponents::new(u64::MAX - 512, 0, 0).unwrap()).unwrap();
    let full = QueueCapacityUsage::new(1, u64::MAX, 1).unwrap();
    assert_eq!(full.replace(finite(u64::MAX), exact, exact), Ok(full));
    assert_eq!(
        full.reserve(finite(u64::MAX), exact),
        Err(QueueCapacityError::ArithmeticOverflow)
    );
    assert_eq!(
        full.refund(finite(u64::MAX), exact),
        Ok(QueueCapacityUsage::new(1, 0, 0).unwrap())
    );
}

#[test]
fn independent_raw_mode_records_refuse_zero_schema_generation_limit_and_unknown_tag() {
    for schema in [0_u8, 2] {
        assert_eq!(
            QueueCapacityMode::decode(&codec::encode(&(schema, 1_u64, 0_u8)).unwrap(), 1),
            Err(QueueCapacityError::InvalidSchema)
        );
    }
    assert_eq!(
        QueueCapacityMode::decode(&codec::encode(&(1_u8, 0_u64, 0_u8)).unwrap(), 1),
        Err(QueueCapacityError::InvalidGeneration)
    );
    assert_eq!(
        QueueCapacityMode::decode(&codec::encode(&(1_u8, 1_u64, 1_u8, 0_u64)).unwrap(), 1),
        Err(QueueCapacityError::InvalidLimit)
    );
    assert!(QueueCapacityMode::decode(&codec::encode(&(1_u8, 1_u64, 2_u8)).unwrap(), 1).is_err());
    assert_eq!(
        QueueCapacityMode::decode(&codec::encode(&(1_u8, 1_u64, 0_u8)).unwrap(), 2),
        Err(QueueCapacityError::GenerationMismatch)
    );
    assert_eq!(
        QueueCapacityMode::non_finite(0),
        Err(QueueCapacityError::InvalidGeneration)
    );
    assert_eq!(
        QueueCapacityMode::finite_v1(0, NonZeroU64::new(1).unwrap()),
        Err(QueueCapacityError::InvalidGeneration)
    );
}

#[test]
fn independent_raw_usage_records_refuse_schema_model_generation_and_count_inconsistency() {
    for schema in [0, 2] {
        assert_eq!(
            QueueCapacityUsage::decode(&raw_usage(schema, 1, 1, 0, 0), 1),
            Err(QueueCapacityError::InvalidSchema)
        );
    }
    for model in [0, 2] {
        assert_eq!(
            QueueCapacityUsage::decode(&raw_usage(1, 1, model, 0, 0), 1),
            Err(QueueCapacityError::InvalidModel)
        );
    }
    assert_eq!(
        QueueCapacityUsage::decode(&raw_usage(1, 0, 1, 0, 0), 1),
        Err(QueueCapacityError::InvalidGeneration)
    );
    assert_eq!(
        QueueCapacityUsage::decode(&raw_usage(1, 1, 1, 0, 0), 0),
        Err(QueueCapacityError::InvalidGeneration)
    );
    assert_eq!(
        QueueCapacityUsage::decode(&raw_usage(1, 1, 1, 0, 0), 2),
        Err(QueueCapacityError::GenerationMismatch)
    );
    for (bytes, count) in [(1, 0), (0, 1), (516, 1), (517, 2), (u64::MAX, u64::MAX)] {
        assert_eq!(
            QueueCapacityUsage::decode(&raw_usage(1, 1, 1, bytes, count), 1),
            Err(QueueCapacityError::InvalidUsage)
        );
    }
}

#[test]
fn independent_raw_charge_records_refuse_schema_model_generation_components_and_overflow() {
    for schema in [0, 2] {
        assert_eq!(
            MessageCharge::decode(&raw_charge(schema, 1, 1, 5, 0, 0, 517), 1),
            Err(QueueCapacityError::InvalidSchema)
        );
    }
    for model in [0, 2] {
        assert_eq!(
            MessageCharge::decode(&raw_charge(1, 1, model, 5, 0, 0, 517), 1),
            Err(QueueCapacityError::InvalidModel)
        );
    }
    assert_eq!(
        MessageCharge::decode(&raw_charge(1, 0, 1, 5, 0, 0, 517), 1),
        Err(QueueCapacityError::InvalidGeneration)
    );
    assert_eq!(
        MessageCharge::decode(&raw_charge(1, 1, 1, 5, 0, 0, 517), 2),
        Err(QueueCapacityError::GenerationMismatch)
    );
    for (producer, session, dead_letter, charged) in [
        (4, 0, 0, 516),
        (5, 1, 0, 518),
        (5, 5, 0, 522),
        (5, 134, 0, 651),
        (5, 0, 80, 517),
        (5, 0, 0, 518),
    ] {
        assert_eq!(
            MessageCharge::decode(
                &raw_charge(1, 1, 1, producer, session, dead_letter, charged),
                1
            ),
            Err(QueueCapacityError::InvalidCharge)
        );
    }
    assert_eq!(
        MessageCharge::decode(&raw_charge(1, 1, 1, u64::MAX, 0, 0, 0), 1),
        Err(QueueCapacityError::ArithmeticOverflow)
    );
}

#[test]
fn independent_noncanonical_varints_trailing_bytes_and_wrong_envelopes_are_refused() {
    let mut overlong_generation = vec![codec::VALUE_FORMAT_V11, 1, 0x81, 0x00, 0];
    assert!(matches!(
        QueueCapacityMode::decode(&overlong_generation, 1),
        Err(QueueCapacityError::NonCanonicalEnvelope
            | QueueCapacityError::Codec(CodecError::Decode))
    ));
    overlong_generation = vec![codec::VALUE_FORMAT_V11, 1, 0x80, 0x00, 0];
    assert!(QueueCapacityMode::decode(&overlong_generation, 1).is_err());
    let non_finite = QueueCapacityMode::non_finite(1).unwrap().encode().unwrap();
    assert_eq!(non_finite, vec![codec::VALUE_FORMAT_V11, 1, 1, 0]);
    for version in 1..codec::VALUE_FORMAT_V11 {
        let mut old = non_finite.clone();
        old[0] = version;
        assert_eq!(
            QueueCapacityMode::decode(&old, 1),
            Err(QueueCapacityError::UnsupportedEnvelope)
        );
    }
    let mut trailing = non_finite;
    trailing.push(0);
    assert_eq!(
        QueueCapacityMode::decode(&trailing, 1),
        Err(QueueCapacityError::Codec(CodecError::Decode))
    );
    assert_eq!(
        QueueCapacityMode::decode(&[], 1),
        Err(QueueCapacityError::Codec(CodecError::EmptyEnvelope))
    );
    assert_eq!(
        QueueCapacityMode::decode(&[0; MAX_CAPACITY_RECORD_BYTES + 1], 1),
        Err(QueueCapacityError::RecordTooLarge)
    );
    assert!(MessageCharge::decode(&raw_usage(1, 1, 1, 0, 0), 1).is_err());
    assert!(QueueCapacityUsage::decode(&raw_charge(1, 1, 1, 5, 0, 0, 517), 1).is_err());
}

#[test]
fn largest_valid_records_are_bounded_canonical_and_generation_checked() {
    for mode in [
        QueueCapacityMode::non_finite(u64::MAX).unwrap(),
        QueueCapacityMode::finite_v1(u64::MAX, NonZeroU64::new(u64::MAX).unwrap()).unwrap(),
    ] {
        let encoded = mode.encode().unwrap();
        assert!(encoded.len() <= MAX_CAPACITY_RECORD_BYTES);
        assert_eq!(QueueCapacityMode::decode(&encoded, u64::MAX), Ok(mode));
        assert_eq!(mode.generation(), u64::MAX);
    }
    let usage = QueueCapacityUsage::new(u64::MAX, u64::MAX, u64::MAX / MIN_CHARGED_BYTES).unwrap();
    let encoded = usage.encode().unwrap();
    assert!(encoded.len() <= MAX_CAPACITY_RECORD_BYTES);
    assert_eq!(QueueCapacityUsage::decode(&encoded, u64::MAX), Ok(usage));
    assert_eq!(usage.generation(), u64::MAX);
    let components = ChargeComponents::new(5, 133, u64::MAX - 5 - 133 - 256).unwrap();
    let charge = MessageCharge::new(u64::MAX, components).unwrap();
    let encoded = charge.encode().unwrap();
    assert!(encoded.len() <= MAX_CAPACITY_RECORD_BYTES);
    assert_eq!(MessageCharge::decode(&encoded, u64::MAX), Ok(charge));
    assert_eq!(charge.generation(), u64::MAX);
    assert_eq!(charge.charged_bytes(), u64::MAX);
}
