use serde::{Deserialize, Serialize};
use thiserror::Error;

mod updates;
pub use updates::{SubscriptionConfigUpdate, TopicConfigUpdate};

use crate::{
    DEFAULT_DUPLICATE_DETECTION_HISTORY_MILLIS, DEFAULT_LOCK_DURATION_MILLIS,
    DEFAULT_MAX_DELIVERY_COUNT, DEFAULT_MAX_MESSAGE_BYTES, MAX_DUPLICATE_DETECTION_HISTORY_MILLIS,
    MAX_LOCK_DURATION_MILLIS, MIN_DUPLICATE_DETECTION_HISTORY_MILLIS, QueueConfig,
};

/// Maximum durable subscriptions on one Standard-tier topic.
pub const MAX_TOPIC_SUBSCRIPTIONS: usize = 2_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicConfig {
    /// `None` means messages do not expire at the topic boundary.
    pub default_time_to_live_millis: Option<u64>,
    pub max_message_bytes: usize,
    /// Creation-only: suppress repeated nonempty identifiers at topic acceptance.
    pub requires_duplicate_detection: bool,
    /// Creation-only history window; validated even when detection is disabled.
    pub duplicate_detection_history_millis: u64,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            default_time_to_live_millis: None,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            requires_duplicate_detection: false,
            duplicate_detection_history_millis: DEFAULT_DUPLICATE_DETECTION_HISTORY_MILLIS,
        }
    }
}

impl TopicConfig {
    pub fn validate(self) -> Result<Self, TopicConfigError> {
        if self.default_time_to_live_millis == Some(0) {
            return Err(TopicConfigError::TimeToLiveTooShort);
        }
        if self.max_message_bytes == 0 {
            return Err(TopicConfigError::MaxMessageBytesTooSmall);
        }
        if self.duplicate_detection_history_millis < MIN_DUPLICATE_DETECTION_HISTORY_MILLIS {
            return Err(TopicConfigError::DuplicateDetectionHistoryTooShort {
                minimum_millis: MIN_DUPLICATE_DETECTION_HISTORY_MILLIS,
            });
        }
        if self.duplicate_detection_history_millis > MAX_DUPLICATE_DETECTION_HISTORY_MILLIS {
            return Err(TopicConfigError::DuplicateDetectionHistoryTooLong {
                maximum_millis: MAX_DUPLICATE_DETECTION_HISTORY_MILLIS,
            });
        }
        Ok(self)
    }

    pub(crate) fn is_legacy_representable(self) -> bool {
        !self.requires_duplicate_detection
            && self.duplicate_detection_history_millis == DEFAULT_DUPLICATE_DETECTION_HISTORY_MILLIS
    }
}

/// Receive-side settings for one match-all subscription.
///
/// Session-aware subscriptions are intentionally absent from this first topic
/// vertical. Adding a boolean that fanout could not honor for mixed
/// subscriptions would expose configuration without implementing its meaning.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    pub lock_duration_millis: u64,
    pub max_delivery_count: u32,
    pub default_time_to_live_millis: Option<u64>,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            lock_duration_millis: DEFAULT_LOCK_DURATION_MILLIS,
            max_delivery_count: DEFAULT_MAX_DELIVERY_COUNT,
            default_time_to_live_millis: None,
        }
    }
}

impl SubscriptionConfig {
    pub fn validate(self) -> Result<Self, SubscriptionConfigError> {
        if self.lock_duration_millis == 0 {
            return Err(SubscriptionConfigError::LockDurationTooShort);
        }
        if self.lock_duration_millis > MAX_LOCK_DURATION_MILLIS {
            return Err(SubscriptionConfigError::LockDurationTooLong {
                maximum_millis: MAX_LOCK_DURATION_MILLIS,
            });
        }
        if self.max_delivery_count == 0 {
            return Err(SubscriptionConfigError::MaxDeliveryCountTooSmall);
        }
        if self.default_time_to_live_millis == Some(0) {
            return Err(SubscriptionConfigError::TimeToLiveTooShort);
        }
        Ok(self)
    }

    pub(crate) fn queue_config(self, topic: TopicConfig) -> QueueConfig {
        QueueConfig {
            lock_duration_millis: self.lock_duration_millis,
            max_delivery_count: self.max_delivery_count,
            default_time_to_live_millis: self.default_time_to_live_millis,
            max_message_bytes: topic.max_message_bytes,
            requires_session: false,
            requires_duplicate_detection: false,
            ..QueueConfig::default()
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum TopicConfigError {
    #[error("default time to live must be at least one millisecond when set")]
    TimeToLiveTooShort,
    #[error("maximum message size must be at least one byte")]
    MaxMessageBytesTooSmall,
    #[error("duplicate detection history must be at least {minimum_millis} milliseconds")]
    DuplicateDetectionHistoryTooShort { minimum_millis: u64 },
    #[error("duplicate detection history exceeds {maximum_millis} milliseconds")]
    DuplicateDetectionHistoryTooLong { maximum_millis: u64 },
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SubscriptionConfigError {
    #[error("lock duration must be at least one millisecond")]
    LockDurationTooShort,
    #[error("lock duration exceeds the {maximum_millis}-millisecond limit")]
    LockDurationTooLong { maximum_millis: u64 },
    #[error("maximum delivery count must be at least one")]
    MaxDeliveryCountTooSmall,
    #[error("default time to live must be at least one millisecond when set")]
    TimeToLiveTooShort,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_topic_and_subscription_configs_are_valid() {
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
    fn subscription_validation_matches_queue_receive_bounds() {
        assert_eq!(
            SubscriptionConfig {
                lock_duration_millis: MAX_LOCK_DURATION_MILLIS + 1,
                ..SubscriptionConfig::default()
            }
            .validate(),
            Err(SubscriptionConfigError::LockDurationTooLong {
                maximum_millis: MAX_LOCK_DURATION_MILLIS,
            })
        );
        assert_eq!(
            SubscriptionConfig {
                max_delivery_count: 0,
                ..SubscriptionConfig::default()
            }
            .validate(),
            Err(SubscriptionConfigError::MaxDeliveryCountTooSmall)
        );
    }
}
