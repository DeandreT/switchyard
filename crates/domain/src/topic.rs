use serde::{Deserialize, Serialize};

use crate::{EntityPath, QueueConfig, QueueConfigError, SubscriptionName};

/// Bounds topology reads and the eventual fanout of a single topic submission.
pub const MAX_TOPIC_SUBSCRIPTIONS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicConfig {
    pub default_time_to_live_millis: Option<u64>,
    pub max_message_bytes: usize,
    pub requires_duplicate_detection: bool,
    pub duplicate_detection_history_time_window_millis: u64,
}

impl Default for TopicConfig {
    fn default() -> Self {
        let defaults = QueueConfig::default();
        Self {
            default_time_to_live_millis: defaults.default_time_to_live_millis,
            max_message_bytes: defaults.max_message_bytes,
            requires_duplicate_detection: defaults.requires_duplicate_detection,
            duplicate_detection_history_time_window_millis: defaults
                .duplicate_detection_history_time_window_millis,
        }
    }
}

impl TopicConfig {
    pub fn validate(self) -> Result<Self, QueueConfigError> {
        self.to_queue_config().validate()?;
        Ok(self)
    }

    /// Reuses the existing ingress limits without introducing receiving settings.
    pub fn to_queue_config(self) -> QueueConfig {
        QueueConfig {
            default_time_to_live_millis: self.default_time_to_live_millis,
            max_message_bytes: self.max_message_bytes,
            requires_duplicate_detection: self.requires_duplicate_detection,
            duplicate_detection_history_time_window_millis: self
                .duplicate_detection_history_time_window_millis,
            ..QueueConfig::default()
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    pub lock_duration_millis: u64,
    pub max_delivery_count: u32,
    pub default_time_to_live_millis: Option<u64>,
    pub max_message_bytes: usize,
    pub requires_session: bool,
    pub dead_lettering_on_message_expiration: bool,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        let defaults = QueueConfig::default();
        Self {
            lock_duration_millis: defaults.lock_duration_millis,
            max_delivery_count: defaults.max_delivery_count,
            default_time_to_live_millis: defaults.default_time_to_live_millis,
            max_message_bytes: defaults.max_message_bytes,
            requires_session: defaults.requires_session,
            dead_lettering_on_message_expiration: defaults.dead_lettering_on_message_expiration,
        }
    }
}

impl SubscriptionConfig {
    pub fn validate(self) -> Result<Self, QueueConfigError> {
        self.to_queue_config().validate()?;
        Ok(self)
    }

    /// Duplicate detection belongs to topic admission, never to individual copies.
    pub fn to_queue_config(self) -> QueueConfig {
        QueueConfig {
            lock_duration_millis: self.lock_duration_millis,
            max_delivery_count: self.max_delivery_count,
            default_time_to_live_millis: self.default_time_to_live_millis,
            max_message_bytes: self.max_message_bytes,
            requires_session: self.requires_session,
            requires_duplicate_detection: false,
            dead_lettering_on_message_expiration: self.dead_lettering_on_message_expiration,
            ..QueueConfig::default()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionDefinition {
    pub name: SubscriptionName,
    pub entity: EntityPath,
    pub config: SubscriptionConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MAX_DUPLICATE_DETECTION_WINDOW_MILLIS, MAX_LOCK_DURATION_MILLIS,
        MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
    };

    #[test]
    fn topology_defaults_match_queue_defaults_and_are_valid() {
        assert_eq!(
            TopicConfig::default().to_queue_config(),
            QueueConfig::default()
        );
        assert_eq!(
            SubscriptionConfig::default().to_queue_config(),
            QueueConfig::default()
        );
        assert_eq!(
            TopicConfig::default().validate(),
            Ok(TopicConfig::default())
        );
        assert_eq!(
            SubscriptionConfig::default().validate(),
            Ok(SubscriptionConfig::default())
        );
    }

    #[test]
    fn topic_projection_preserves_ingress_limits_and_duplicate_detection() {
        let config = TopicConfig {
            default_time_to_live_millis: Some(25_000),
            max_message_bytes: 16_384,
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
        };
        assert_eq!(config.validate(), Ok(config));
        assert_eq!(
            config.to_queue_config(),
            QueueConfig {
                default_time_to_live_millis: Some(25_000),
                max_message_bytes: 16_384,
                requires_duplicate_detection: true,
                duplicate_detection_history_time_window_millis:
                    MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
                ..QueueConfig::default()
            }
        );
    }

    #[test]
    fn subscription_projection_preserves_receive_limits_without_duplicate_detection() {
        let config = SubscriptionConfig {
            lock_duration_millis: MAX_LOCK_DURATION_MILLIS,
            max_delivery_count: 3,
            default_time_to_live_millis: Some(5_000),
            max_message_bytes: 1_024,
            requires_session: true,
            dead_lettering_on_message_expiration: true,
        };
        assert_eq!(config.validate(), Ok(config));
        assert_eq!(
            config.to_queue_config(),
            QueueConfig {
                lock_duration_millis: MAX_LOCK_DURATION_MILLIS,
                max_delivery_count: 3,
                default_time_to_live_millis: Some(5_000),
                max_message_bytes: 1_024,
                requires_session: true,
                requires_duplicate_detection: false,
                dead_lettering_on_message_expiration: true,
                ..QueueConfig::default()
            }
        );
    }

    #[test]
    fn topic_validation_reuses_all_ingress_config_boundaries() {
        let default = TopicConfig::default();
        for (config, expected) in [
            (
                TopicConfig {
                    max_message_bytes: 0,
                    ..default
                },
                QueueConfigError::MaxMessageBytesTooSmall,
            ),
            (
                TopicConfig {
                    default_time_to_live_millis: Some(0),
                    ..default
                },
                QueueConfigError::TimeToLiveTooShort,
            ),
            (
                TopicConfig {
                    duplicate_detection_history_time_window_millis:
                        MIN_DUPLICATE_DETECTION_WINDOW_MILLIS - 1,
                    ..default
                },
                QueueConfigError::DuplicateDetectionWindowTooShort {
                    minimum_millis: MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
                },
            ),
            (
                TopicConfig {
                    duplicate_detection_history_time_window_millis:
                        MAX_DUPLICATE_DETECTION_WINDOW_MILLIS + 1,
                    ..default
                },
                QueueConfigError::DuplicateDetectionWindowTooLong {
                    maximum_millis: MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
                },
            ),
        ] {
            assert_eq!(config.validate(), Err(expected));
        }
        assert!(
            TopicConfig {
                default_time_to_live_millis: Some(1),
                max_message_bytes: 1,
                duplicate_detection_history_time_window_millis:
                    MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
                ..default
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn subscription_validation_reuses_all_receiving_config_boundaries() {
        let default = SubscriptionConfig::default();
        for (config, expected) in [
            (
                SubscriptionConfig {
                    lock_duration_millis: 0,
                    ..default
                },
                QueueConfigError::LockDurationTooShort,
            ),
            (
                SubscriptionConfig {
                    lock_duration_millis: MAX_LOCK_DURATION_MILLIS + 1,
                    ..default
                },
                QueueConfigError::LockDurationTooLong {
                    maximum_millis: MAX_LOCK_DURATION_MILLIS,
                },
            ),
            (
                SubscriptionConfig {
                    max_delivery_count: 0,
                    ..default
                },
                QueueConfigError::MaxDeliveryCountTooSmall,
            ),
            (
                SubscriptionConfig {
                    max_message_bytes: 0,
                    ..default
                },
                QueueConfigError::MaxMessageBytesTooSmall,
            ),
            (
                SubscriptionConfig {
                    default_time_to_live_millis: Some(0),
                    ..default
                },
                QueueConfigError::TimeToLiveTooShort,
            ),
        ] {
            assert_eq!(config.validate(), Err(expected));
        }
        assert!(
            SubscriptionConfig {
                lock_duration_millis: 1,
                max_delivery_count: 1,
                default_time_to_live_millis: Some(1),
                max_message_bytes: 1,
                ..default
            }
            .validate()
            .is_ok()
        );
    }
}
