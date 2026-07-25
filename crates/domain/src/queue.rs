use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Longest lock Service Bus accepts, and the value Switchyard enforces so that
/// a client cannot pin a message indefinitely.
pub const MAX_LOCK_DURATION_MILLIS: u64 = 5 * 60 * 1_000;
pub const DEFAULT_LOCK_DURATION_MILLIS: u64 = 60 * 1_000;
pub const DEFAULT_MAX_DELIVERY_COUNT: u32 = 10;
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueConfig {
    pub lock_duration_millis: u64,
    pub max_delivery_count: u32,
    /// `None` means messages never expire, matching the Service Bus default of
    /// an effectively unbounded time to live.
    pub default_time_to_live_millis: Option<u64>,
    pub max_message_bytes: usize,
    /// Accepted and stored, but session ordering is not yet enforced.
    pub requires_session: bool,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            lock_duration_millis: DEFAULT_LOCK_DURATION_MILLIS,
            max_delivery_count: DEFAULT_MAX_DELIVERY_COUNT,
            default_time_to_live_millis: None,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            requires_session: false,
        }
    }
}

impl QueueConfig {
    pub fn validate(self) -> Result<Self, QueueConfigError> {
        if self.lock_duration_millis == 0 {
            return Err(QueueConfigError::LockDurationTooShort);
        }
        if self.lock_duration_millis > MAX_LOCK_DURATION_MILLIS {
            return Err(QueueConfigError::LockDurationTooLong {
                maximum_millis: MAX_LOCK_DURATION_MILLIS,
            });
        }
        if self.max_delivery_count == 0 {
            return Err(QueueConfigError::MaxDeliveryCountTooSmall);
        }
        if self.max_message_bytes == 0 {
            return Err(QueueConfigError::MaxMessageBytesTooSmall);
        }
        if self.default_time_to_live_millis == Some(0) {
            return Err(QueueConfigError::TimeToLiveTooShort);
        }
        Ok(self)
    }
}

/// Per-entity replicated counters.
///
/// Sequence numbers and lock tokens are allocated here rather than generated
/// locally so that every replica applying the same command derives the same
/// identifiers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueCounters {
    pub next_sequence: u64,
    pub next_lock_token: u64,
}

impl Default for QueueCounters {
    fn default() -> Self {
        Self {
            next_sequence: 1,
            next_lock_token: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum QueueConfigError {
    #[error("lock duration must be at least one millisecond")]
    LockDurationTooShort,
    #[error("lock duration exceeds the {maximum_millis}-millisecond limit")]
    LockDurationTooLong { maximum_millis: u64 },
    #[error("maximum delivery count must be at least one")]
    MaxDeliveryCountTooSmall,
    #[error("maximum message size must be at least one byte")]
    MaxMessageBytesTooSmall,
    #[error("default time to live must be at least one millisecond when set")]
    TimeToLiveTooShort,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_configuration_is_valid() {
        assert_eq!(
            QueueConfig::default().validate(),
            Ok(QueueConfig::default())
        );
    }

    #[test]
    fn rejects_a_lock_that_outlives_the_service_bus_limit() {
        let config = QueueConfig {
            lock_duration_millis: MAX_LOCK_DURATION_MILLIS + 1,
            ..QueueConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(QueueConfigError::LockDurationTooLong {
                maximum_millis: MAX_LOCK_DURATION_MILLIS
            })
        );
    }

    #[test]
    fn rejects_a_queue_that_can_never_deliver() {
        let config = QueueConfig {
            max_delivery_count: 0,
            ..QueueConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(QueueConfigError::MaxDeliveryCountTooSmall)
        );
    }

    #[test]
    fn counters_start_at_one() {
        let counters = QueueCounters::default();
        assert_eq!(counters.next_sequence, 1);
        assert_eq!(counters.next_lock_token, 1);
    }
}
