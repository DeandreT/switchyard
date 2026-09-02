use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Longest lock Service Bus accepts, and the value Switchyard enforces so that
/// a client cannot pin a message indefinitely.
pub const MAX_LOCK_DURATION_MILLIS: u64 = 5 * 60 * 1_000;
pub const DEFAULT_LOCK_DURATION_MILLIS: u64 = 60 * 1_000;
pub const DEFAULT_MAX_DELIVERY_COUNT: u32 = 10;
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 256 * 1024;
/// Service Bus default when duplicate detection is configured without an
/// explicit history window.
pub const DEFAULT_DUPLICATE_DETECTION_HISTORY_MILLIS: u64 = 10 * 60 * 1_000;
pub const MIN_DUPLICATE_DETECTION_HISTORY_MILLIS: u64 = 20 * 1_000;
pub const MAX_DUPLICATE_DETECTION_HISTORY_MILLIS: u64 = 7 * 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueConfig {
    pub lock_duration_millis: u64,
    pub max_delivery_count: u32,
    /// `None` means messages never expire, matching the Service Bus default of
    /// an effectively unbounded time to live.
    pub default_time_to_live_millis: Option<u64>,
    pub max_message_bytes: usize,
    /// When set, every message carries a session identifier and is only
    /// delivered to a receiver holding that session's lock. Ordering within a
    /// session is the only FIFO guarantee the broker makes.
    pub requires_session: bool,
    /// Whether repeated non-empty message identifiers are accepted but
    /// suppressed within this entity's configured history window.
    pub requires_duplicate_detection: bool,
    /// How long an accepted message identifier remains a duplicate.
    pub duplicate_detection_history_millis: u64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            lock_duration_millis: DEFAULT_LOCK_DURATION_MILLIS,
            max_delivery_count: DEFAULT_MAX_DELIVERY_COUNT,
            default_time_to_live_millis: None,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            requires_session: false,
            requires_duplicate_detection: false,
            duplicate_detection_history_millis: DEFAULT_DUPLICATE_DETECTION_HISTORY_MILLIS,
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
        if self.duplicate_detection_history_millis < MIN_DUPLICATE_DETECTION_HISTORY_MILLIS {
            return Err(QueueConfigError::DuplicateDetectionHistoryTooShort {
                minimum_millis: MIN_DUPLICATE_DETECTION_HISTORY_MILLIS,
            });
        }
        if self.duplicate_detection_history_millis > MAX_DUPLICATE_DETECTION_HISTORY_MILLIS {
            return Err(QueueConfigError::DuplicateDetectionHistoryTooLong {
                maximum_millis: MAX_DUPLICATE_DETECTION_HISTORY_MILLIS,
            });
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
    #[error("duplicate-detection history must be at least {minimum_millis} milliseconds")]
    DuplicateDetectionHistoryTooShort { minimum_millis: u64 },
    #[error("duplicate-detection history cannot exceed {maximum_millis} milliseconds")]
    DuplicateDetectionHistoryTooLong { maximum_millis: u64 },
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

    #[test]
    fn duplicate_detection_history_enforces_service_bus_bounds() {
        let too_short = QueueConfig {
            duplicate_detection_history_millis: MIN_DUPLICATE_DETECTION_HISTORY_MILLIS - 1,
            ..QueueConfig::default()
        };
        assert_eq!(
            too_short.validate(),
            Err(QueueConfigError::DuplicateDetectionHistoryTooShort {
                minimum_millis: MIN_DUPLICATE_DETECTION_HISTORY_MILLIS,
            })
        );

        let too_long = QueueConfig {
            duplicate_detection_history_millis: MAX_DUPLICATE_DETECTION_HISTORY_MILLIS + 1,
            ..QueueConfig::default()
        };
        assert_eq!(
            too_long.validate(),
            Err(QueueConfigError::DuplicateDetectionHistoryTooLong {
                maximum_millis: MAX_DUPLICATE_DETECTION_HISTORY_MILLIS,
            })
        );

        for history in [
            MIN_DUPLICATE_DETECTION_HISTORY_MILLIS,
            MAX_DUPLICATE_DETECTION_HISTORY_MILLIS,
        ] {
            assert!(
                QueueConfig {
                    duplicate_detection_history_millis: history,
                    ..QueueConfig::default()
                }
                .validate()
                .is_ok()
            );
        }
    }
}
