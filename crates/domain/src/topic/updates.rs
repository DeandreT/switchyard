use serde::{Deserialize, Serialize};

use crate::{BrokerError, QueueTimeToLiveUpdate};

use super::{SubscriptionConfig, TopicConfig};

/// Replaces supplied topic settings without rewriting retained messages or history.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopicConfigUpdate {
    pub default_time_to_live_millis: Option<QueueTimeToLiveUpdate>,
    pub max_message_bytes: Option<usize>,
    pub requires_duplicate_detection: Option<bool>,
    pub duplicate_detection_history_time_window_millis: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicImmutableProperty {
    RequiresDuplicateDetection,
}

impl std::fmt::Display for TopicImmutableProperty {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RequiresDuplicateDetection => "requires_duplicate_detection",
        })
    }
}

impl TopicConfigUpdate {
    pub(crate) fn apply_to(self, current: TopicConfig) -> Result<TopicConfig, BrokerError> {
        if self
            .requires_duplicate_detection
            .is_some_and(|value| value != current.requires_duplicate_detection)
        {
            return Err(BrokerError::TopicPropertyIsImmutable {
                property: TopicImmutableProperty::RequiresDuplicateDetection,
            });
        }
        TopicConfig {
            default_time_to_live_millis: updated_ttl(
                self.default_time_to_live_millis,
                current.default_time_to_live_millis,
            ),
            max_message_bytes: self.max_message_bytes.unwrap_or(current.max_message_bytes),
            duplicate_detection_history_time_window_millis: self
                .duplicate_detection_history_time_window_millis
                .unwrap_or(current.duplicate_detection_history_time_window_millis),
            ..current
        }
        .validate()
        .map_err(BrokerError::TopicConfig)
    }
}

/// Replaces supplied subscription settings while preserving session index policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionConfigUpdate {
    pub lock_duration_millis: Option<u64>,
    pub max_delivery_count: Option<u32>,
    pub default_time_to_live_millis: Option<QueueTimeToLiveUpdate>,
    pub max_message_bytes: Option<usize>,
    pub requires_session: Option<bool>,
    pub dead_lettering_on_message_expiration: Option<bool>,
    pub dead_lettering_on_filter_evaluation_exceptions: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionImmutableProperty {
    RequiresSession,
}

impl std::fmt::Display for SubscriptionImmutableProperty {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RequiresSession => "requires_session",
        })
    }
}

impl SubscriptionConfigUpdate {
    pub(crate) fn apply_to(
        self,
        current: SubscriptionConfig,
    ) -> Result<SubscriptionConfig, BrokerError> {
        if self
            .requires_session
            .is_some_and(|value| value != current.requires_session)
        {
            return Err(BrokerError::SubscriptionPropertyIsImmutable {
                property: SubscriptionImmutableProperty::RequiresSession,
            });
        }
        SubscriptionConfig {
            lock_duration_millis: self
                .lock_duration_millis
                .unwrap_or(current.lock_duration_millis),
            max_delivery_count: self
                .max_delivery_count
                .unwrap_or(current.max_delivery_count),
            default_time_to_live_millis: updated_ttl(
                self.default_time_to_live_millis,
                current.default_time_to_live_millis,
            ),
            max_message_bytes: self.max_message_bytes.unwrap_or(current.max_message_bytes),
            dead_lettering_on_message_expiration: self
                .dead_lettering_on_message_expiration
                .unwrap_or(current.dead_lettering_on_message_expiration),
            dead_lettering_on_filter_evaluation_exceptions: self
                .dead_lettering_on_filter_evaluation_exceptions
                .unwrap_or(current.dead_lettering_on_filter_evaluation_exceptions),
            ..current
        }
        .validate()
        .map_err(BrokerError::SubscriptionConfig)
    }
}

fn updated_ttl(update: Option<QueueTimeToLiveUpdate>, current: Option<u64>) -> Option<u64> {
    match update {
        None => current,
        Some(QueueTimeToLiveUpdate::Unlimited) => None,
        Some(QueueTimeToLiveUpdate::Finite { millis }) => Some(millis),
    }
}

#[cfg(test)]
mod tests;
