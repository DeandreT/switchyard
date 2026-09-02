use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    DEFAULT_LOCK_DURATION_MILLIS, DEFAULT_MAX_DELIVERY_COUNT, DEFAULT_MAX_MESSAGE_BYTES,
    MAX_LOCK_DURATION_MILLIS, QueueConfig,
};

/// Maximum durable subscriptions on one Standard-tier topic.
pub const MAX_TOPIC_SUBSCRIPTIONS: usize = 2_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicConfig {
    /// `None` means messages do not expire at the topic boundary.
    pub default_time_to_live_millis: Option<u64>,
    pub max_message_bytes: usize,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            default_time_to_live_millis: None,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
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
        Ok(self)
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
