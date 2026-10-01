use super::*;

#[test]
fn filter_exception_policy_defaults_true_and_is_not_a_queue_setting() {
    let defaults = SubscriptionConfig::default();
    assert!(defaults.dead_lettering_on_filter_evaluation_exceptions);
    let disabled = SubscriptionConfig {
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..defaults
    };
    assert_ne!(defaults, disabled);
    assert_eq!(defaults.to_queue_config(), disabled.to_queue_config());
    assert_eq!(
        defaults.to_queue_config().dead_letter_shadow(),
        disabled.to_queue_config().dead_letter_shadow()
    );
    assert_eq!(disabled.validate(), Ok(disabled));
}

#[test]
fn legacy_six_field_records_select_the_explicit_true_default() -> Result<(), CodecError> {
    for lifetime in [None, Some(0), Some(12_345)] {
        let legacy = SubscriptionConfigV9 {
            lock_duration_millis: 777,
            max_delivery_count: 17,
            default_time_to_live_millis: lifetime,
            max_message_bytes: 2_048,
            requires_session: true,
            dead_lettering_on_message_expiration: true,
        };
        let expected = SubscriptionConfig {
            lock_duration_millis: legacy.lock_duration_millis,
            max_delivery_count: legacy.max_delivery_count,
            default_time_to_live_millis: legacy.default_time_to_live_millis,
            max_message_bytes: legacy.max_message_bytes,
            requires_session: legacy.requires_session,
            dead_lettering_on_message_expiration: legacy.dead_lettering_on_message_expiration,
            dead_lettering_on_filter_evaluation_exceptions: true,
        };
        let mut bytes = codec::encode(&legacy)?;
        assert_eq!(bytes[0], codec::VALUE_FORMAT_V10);
        assert_eq!(SubscriptionConfig::decode(&bytes), Err(CodecError::Decode));
        for version in codec::VALUE_FORMAT_V1..=codec::VALUE_FORMAT_V9 {
            bytes[0] = version;
            assert_eq!(SubscriptionConfig::decode(&bytes)?, expected);
        }
    }
    Ok(())
}

#[test]
fn current_true_and_false_policies_round_trip_but_cannot_be_relabelled_legacy()
-> Result<(), CodecError> {
    for enabled in [true, false] {
        let original = SubscriptionConfig {
            dead_lettering_on_filter_evaluation_exceptions: enabled,
            requires_session: true,
            ..SubscriptionConfig::default()
        };
        let mut bytes = codec::encode(&original)?;
        assert_eq!(bytes[0], codec::VALUE_FORMAT_V10);
        assert_eq!(SubscriptionConfig::decode(&bytes)?, original);
        assert_eq!(codec::decode::<SubscriptionConfig>(&bytes)?, original);
        for version in codec::VALUE_FORMAT_V1..=codec::VALUE_FORMAT_V9 {
            bytes[0] = version;
            assert_eq!(SubscriptionConfig::decode(&bytes), Err(CodecError::Decode));
        }
    }
    Ok(())
}

#[test]
fn malformed_current_and_unknown_policy_records_are_refused() -> Result<(), CodecError> {
    let mut bytes = codec::encode(&SubscriptionConfig::default())?;
    assert_eq!(bytes.pop(), Some(1));
    assert_eq!(SubscriptionConfig::decode(&bytes), Err(CodecError::Decode));
    bytes.push(2);
    assert_eq!(SubscriptionConfig::decode(&bytes), Err(CodecError::Decode));
    bytes.pop();
    bytes.push(0);
    bytes.push(1);
    assert_eq!(SubscriptionConfig::decode(&bytes), Err(CodecError::Decode));
    assert_eq!(
        SubscriptionConfig::decode(&[]),
        Err(CodecError::EmptyEnvelope)
    );
    bytes[0] = codec::VALUE_FORMAT_V10 + 1;
    assert_eq!(
        SubscriptionConfig::decode(&bytes),
        Err(CodecError::UnsupportedVersion {
            version: codec::VALUE_FORMAT_V10 + 1
        })
    );
    Ok(())
}
