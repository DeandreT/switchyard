use super::*;
use crate::{
    Command, CommandKind, EntityPath, MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
    MAX_LOCK_DURATION_MILLIS, MIN_DUPLICATE_DETECTION_WINDOW_MILLIS, NamespaceName,
    QueueConfigError, QueueConfigUpdate, RuleFilter, RuleName, SubscriptionName, Timestamp,
};

#[test]
fn empty_and_immutable_restatements_preserve_configuration() -> Result<(), BrokerError> {
    for enabled in [false, true] {
        let topic = TopicConfig {
            requires_duplicate_detection: enabled,
            ..TopicConfig::default()
        };
        assert_eq!(TopicConfigUpdate::default().apply_to(topic)?, topic);
        assert_eq!(
            TopicConfigUpdate {
                requires_duplicate_detection: Some(enabled),
                ..TopicConfigUpdate::default()
            }
            .apply_to(topic)?,
            topic
        );
        let subscription = SubscriptionConfig {
            requires_session: enabled,
            ..SubscriptionConfig::default()
        };
        assert_eq!(
            SubscriptionConfigUpdate::default().apply_to(subscription)?,
            subscription
        );
        assert_eq!(
            SubscriptionConfigUpdate {
                requires_session: Some(enabled),
                ..SubscriptionConfigUpdate::default()
            }
            .apply_to(subscription)?,
            subscription
        );
    }
    Ok(())
}

#[test]
fn mutable_fields_replace_only_their_targeted_settings() -> Result<(), BrokerError> {
    let topic = TopicConfig {
        requires_duplicate_detection: true,
        ..TopicConfig::default()
    };
    assert_eq!(
        TopicConfigUpdate {
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 19 }),
            max_message_bytes: Some(123),
            duplicate_detection_history_time_window_millis: Some(
                MIN_DUPLICATE_DETECTION_WINDOW_MILLIS
            ),
            ..TopicConfigUpdate::default()
        }
        .apply_to(topic)?,
        TopicConfig {
            default_time_to_live_millis: Some(19),
            max_message_bytes: 123,
            duplicate_detection_history_time_window_millis: MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
            ..topic
        }
    );
    let subscription = SubscriptionConfig {
        requires_session: true,
        ..SubscriptionConfig::default()
    };
    let expected = SubscriptionConfig {
        lock_duration_millis: 1,
        max_delivery_count: 2,
        default_time_to_live_millis: Some(3),
        max_message_bytes: 4,
        dead_lettering_on_message_expiration: true,
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..subscription
    };
    assert_eq!(
        SubscriptionConfigUpdate {
            lock_duration_millis: Some(1),
            max_delivery_count: Some(2),
            default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 3 }),
            max_message_bytes: Some(4),
            dead_lettering_on_message_expiration: Some(true),
            dead_lettering_on_filter_evaluation_exceptions: Some(false),
            ..SubscriptionConfigUpdate::default()
        }
        .apply_to(subscription)?,
        expected
    );
    assert!(expected.to_queue_config().requires_session);
    assert!(!expected.to_queue_config().requires_duplicate_detection);
    let shadow = expected.to_queue_config().dead_letter_shadow();
    assert!(!shadow.requires_session);
    assert!(!shadow.dead_lettering_on_message_expiration);
    assert_eq!(shadow.default_time_to_live_millis, None);
    Ok(())
}

#[test]
fn ttl_omission_unlimited_and_finite_have_distinct_effects() -> Result<(), BrokerError> {
    for (update, expected) in [
        (None, Some(9)),
        (Some(QueueTimeToLiveUpdate::Unlimited), None),
        (Some(QueueTimeToLiveUpdate::Finite { millis: 7 }), Some(7)),
    ] {
        let topic = TopicConfig {
            default_time_to_live_millis: Some(9),
            ..TopicConfig::default()
        };
        assert_eq!(
            TopicConfigUpdate {
                default_time_to_live_millis: update,
                ..TopicConfigUpdate::default()
            }
            .apply_to(topic)?
            .default_time_to_live_millis,
            expected
        );
        let subscription = SubscriptionConfig {
            default_time_to_live_millis: Some(9),
            ..SubscriptionConfig::default()
        };
        assert_eq!(
            SubscriptionConfigUpdate {
                default_time_to_live_millis: update,
                ..SubscriptionConfigUpdate::default()
            }
            .apply_to(subscription)?
            .default_time_to_live_millis,
            expected
        );
    }
    Ok(())
}

#[test]
fn immutable_toggles_precede_invalid_mutable_values() {
    for enabled in [false, true] {
        assert_eq!(
            TopicConfigUpdate {
                requires_duplicate_detection: Some(!enabled),
                max_message_bytes: Some(0),
                ..TopicConfigUpdate::default()
            }
            .apply_to(TopicConfig {
                requires_duplicate_detection: enabled,
                ..TopicConfig::default()
            }),
            Err(BrokerError::TopicPropertyIsImmutable {
                property: TopicImmutableProperty::RequiresDuplicateDetection,
            })
        );
        assert_eq!(
            SubscriptionConfigUpdate {
                requires_session: Some(!enabled),
                lock_duration_millis: Some(0),
                ..SubscriptionConfigUpdate::default()
            }
            .apply_to(SubscriptionConfig {
                requires_session: enabled,
                ..SubscriptionConfig::default()
            }),
            Err(BrokerError::SubscriptionPropertyIsImmutable {
                property: SubscriptionImmutableProperty::RequiresSession,
            })
        );
    }
}

#[test]
fn numeric_validation_keeps_topic_and_subscription_error_provenance() {
    for (update, error) in [
        (
            TopicConfigUpdate {
                max_message_bytes: Some(0),
                ..TopicConfigUpdate::default()
            },
            QueueConfigError::MaxMessageBytesTooSmall,
        ),
        (
            TopicConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 0 }),
                ..TopicConfigUpdate::default()
            },
            QueueConfigError::TimeToLiveTooShort,
        ),
        (
            TopicConfigUpdate {
                duplicate_detection_history_time_window_millis: Some(
                    MIN_DUPLICATE_DETECTION_WINDOW_MILLIS - 1,
                ),
                ..TopicConfigUpdate::default()
            },
            QueueConfigError::DuplicateDetectionWindowTooShort {
                minimum_millis: MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
            },
        ),
        (
            TopicConfigUpdate {
                duplicate_detection_history_time_window_millis: Some(
                    MAX_DUPLICATE_DETECTION_WINDOW_MILLIS + 1,
                ),
                ..TopicConfigUpdate::default()
            },
            QueueConfigError::DuplicateDetectionWindowTooLong {
                maximum_millis: MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
            },
        ),
    ] {
        assert_eq!(
            update.apply_to(TopicConfig::default()),
            Err(BrokerError::TopicConfig(error))
        );
    }
    for (update, error) in [
        (
            SubscriptionConfigUpdate {
                lock_duration_millis: Some(0),
                ..SubscriptionConfigUpdate::default()
            },
            QueueConfigError::LockDurationTooShort,
        ),
        (
            SubscriptionConfigUpdate {
                lock_duration_millis: Some(MAX_LOCK_DURATION_MILLIS + 1),
                ..SubscriptionConfigUpdate::default()
            },
            QueueConfigError::LockDurationTooLong {
                maximum_millis: MAX_LOCK_DURATION_MILLIS,
            },
        ),
        (
            SubscriptionConfigUpdate {
                max_delivery_count: Some(0),
                ..SubscriptionConfigUpdate::default()
            },
            QueueConfigError::MaxDeliveryCountTooSmall,
        ),
        (
            SubscriptionConfigUpdate {
                max_message_bytes: Some(0),
                ..SubscriptionConfigUpdate::default()
            },
            QueueConfigError::MaxMessageBytesTooSmall,
        ),
        (
            SubscriptionConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 0 }),
                ..SubscriptionConfigUpdate::default()
            },
            QueueConfigError::TimeToLiveTooShort,
        ),
    ] {
        assert_eq!(
            update.apply_to(SubscriptionConfig::default()),
            Err(BrokerError::SubscriptionConfig(error))
        );
    }
}

#[test]
fn command_ordinals_append_and_ttl_patch_encodings_remain_distinct()
-> Result<(), Box<dyn std::error::Error>> {
    for (update, expected) in [
        (TopicConfigUpdate::default(), vec![34, 0, 0, 0, 0]),
        (
            TopicConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
                ..TopicConfigUpdate::default()
            },
            vec![34, 1, 0, 0, 0, 0],
        ),
        (
            TopicConfigUpdate {
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 7 }),
                ..TopicConfigUpdate::default()
            },
            vec![34, 1, 1, 7, 0, 0, 0],
        ),
    ] {
        let kind = CommandKind::UpdateTopic { update };
        assert_eq!(postcard::to_stdvec(&kind)?, expected);
        assert_eq!(postcard::from_bytes::<CommandKind>(&expected)?, kind);
    }
    let kinds = [
        (
            28,
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate::default(),
            },
        ),
        (
            29,
            CommandKind::SendBatch {
                messages: Vec::new(),
            },
        ),
        (
            30,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        ),
        (
            31,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("Alpha")?,
                config: SubscriptionConfig::default(),
            },
        ),
        (
            32,
            CommandKind::CreateRule {
                subscription: SubscriptionName::new("Alpha")?,
                name: RuleName::new("$Default")?,
                filter: RuleFilter::True,
            },
        ),
        (
            33,
            CommandKind::DeleteRule {
                subscription: SubscriptionName::new("Alpha")?,
                name: RuleName::new("$Default")?,
            },
        ),
        (
            34,
            CommandKind::UpdateTopic {
                update: TopicConfigUpdate::default(),
            },
        ),
        (
            35,
            CommandKind::UpdateSubscription {
                name: SubscriptionName::new("Alpha")?,
                update: SubscriptionConfigUpdate::default(),
            },
        ),
    ];
    for (ordinal, kind) in kinds {
        let payload = postcard::to_stdvec(&kind)?;
        assert_eq!(payload.first(), Some(&ordinal));
        assert_eq!(postcard::from_bytes::<CommandKind>(&payload)?, kind);
        let command = Command::new(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            Timestamp::from_millis(11),
            kind,
        );
        assert_eq!(
            postcard::from_bytes::<Command>(&postcard::to_stdvec(&command)?)?,
            command
        );
    }
    Ok(())
}

#[test]
fn immutable_property_names_are_specific_to_the_topology_kind() {
    assert_eq!(
        TopicImmutableProperty::RequiresDuplicateDetection.to_string(),
        "requires_duplicate_detection"
    );
    assert_eq!(
        SubscriptionImmutableProperty::RequiresSession.to_string(),
        "requires_session"
    );
}

#[test]
fn complete_patches_round_trip_without_changing_stored_configuration_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    let topic = TopicConfigUpdate {
        default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Unlimited),
        max_message_bytes: Some(17),
        requires_duplicate_detection: Some(true),
        duplicate_detection_history_time_window_millis: Some(MAX_DUPLICATE_DETECTION_WINDOW_MILLIS),
    };
    let subscription = SubscriptionConfigUpdate {
        lock_duration_millis: Some(11),
        max_delivery_count: Some(12),
        default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 13 }),
        max_message_bytes: Some(14),
        requires_session: Some(false),
        dead_lettering_on_message_expiration: Some(true),
        dead_lettering_on_filter_evaluation_exceptions: Some(false),
    };
    assert_eq!(
        postcard::from_bytes::<TopicConfigUpdate>(&postcard::to_stdvec(&topic)?)?,
        topic
    );
    assert_eq!(
        postcard::from_bytes::<SubscriptionConfigUpdate>(&postcard::to_stdvec(&subscription)?)?,
        subscription
    );
    let current = SubscriptionConfig::default();
    assert_eq!(
        SubscriptionConfig::decode(&crate::codec::encode(&current)?)?,
        current
    );
    Ok(())
}
