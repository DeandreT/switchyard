use serde::{Deserialize, Serialize};

use crate::{
    QueueTimeToLiveUpdate, SubscriptionConfig, SubscriptionConfigError, TopicConfig,
    TopicConfigError,
};

/// Partial parent-topic profile update. An absent field retains its value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TopicConfigUpdate {
    pub default_time_to_live_millis: Option<QueueTimeToLiveUpdate>,
    pub max_message_bytes: Option<usize>,
}

impl TopicConfigUpdate {
    pub fn apply(self, current: TopicConfig) -> Result<TopicConfig, TopicConfigError> {
        current.validate()?;
        TopicConfig {
            default_time_to_live_millis: lifetime(
                self.default_time_to_live_millis,
                current.default_time_to_live_millis,
            ),
            max_message_bytes: self.max_message_bytes.unwrap_or(current.max_message_bytes),
            ..current
        }
        .validate()
    }
}

/// Independent receive-side settings for one subscription, addressed by its parent.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SubscriptionConfigUpdate {
    pub lock_duration_millis: Option<u64>,
    pub max_delivery_count: Option<u32>,
    pub default_time_to_live_millis: Option<QueueTimeToLiveUpdate>,
}

impl SubscriptionConfigUpdate {
    pub fn apply(
        self,
        current: SubscriptionConfig,
    ) -> Result<SubscriptionConfig, SubscriptionConfigError> {
        current.validate()?;
        SubscriptionConfig {
            lock_duration_millis: self
                .lock_duration_millis
                .unwrap_or(current.lock_duration_millis),
            max_delivery_count: self
                .max_delivery_count
                .unwrap_or(current.max_delivery_count),
            default_time_to_live_millis: lifetime(
                self.default_time_to_live_millis,
                current.default_time_to_live_millis,
            ),
        }
        .validate()
    }
}

fn lifetime(update: Option<QueueTimeToLiveUpdate>, current: Option<u64>) -> Option<u64> {
    match update {
        Some(QueueTimeToLiveUpdate::Finite { millis }) => Some(millis),
        Some(QueueTimeToLiveUpdate::Unlimited) => None,
        None => current,
    }
}
