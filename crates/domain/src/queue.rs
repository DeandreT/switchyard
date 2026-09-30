use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CodecError, codec};

/// Longest lock Service Bus accepts, and the value Switchyard enforces so that
/// a client cannot pin a message indefinitely.
pub const MAX_LOCK_DURATION_MILLIS: u64 = 5 * 60 * 1_000;
pub const DEFAULT_LOCK_DURATION_MILLIS: u64 = 60 * 1_000;
pub const DEFAULT_MAX_DELIVERY_COUNT: u32 = 10;
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 256 * 1024;
pub const MIN_DUPLICATE_DETECTION_WINDOW_MILLIS: u64 = 20 * 1_000;
pub const DEFAULT_DUPLICATE_DETECTION_WINDOW_MILLIS: u64 = 10 * 60 * 1_000;
pub const MAX_DUPLICATE_DETECTION_WINDOW_MILLIS: u64 = 7 * 24 * 60 * 60 * 1_000;

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
    /// Drops later submissions with the same nonempty message identifier
    /// within the history window, while acknowledging them as accepted.
    pub requires_duplicate_detection: bool,
    /// History lifetime measured from the original accepted submission, not
    /// from the latest duplicate or a scheduled message's activation.
    pub duplicate_detection_history_time_window_millis: u64,
}

/// Versions 1 through 5, before queues could retain duplicate-detection history.
#[derive(Deserialize)]
struct QueueConfigV5 {
    lock_duration_millis: u64,
    max_delivery_count: u32,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: usize,
    requires_session: bool,
}

impl From<QueueConfigV5> for QueueConfig {
    fn from(config: QueueConfigV5) -> Self {
        Self {
            lock_duration_millis: config.lock_duration_millis,
            max_delivery_count: config.max_delivery_count,
            default_time_to_live_millis: config.default_time_to_live_millis,
            max_message_bytes: config.max_message_bytes,
            requires_session: config.requires_session,
            ..Self::default()
        }
    }
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
            duplicate_detection_history_time_window_millis:
                DEFAULT_DUPLICATE_DETECTION_WINDOW_MILLIS,
        }
    }
}

impl QueueConfig {
    /// Decodes a stored configuration, preserving older settings and keeping
    /// duplicate detection disabled on queues that predate the feature.
    pub fn decode(envelope: &[u8]) -> Result<Self, CodecError> {
        let (version, payload) = codec::split(envelope)?;
        match version {
            codec::VALUE_FORMAT_V1..=codec::VALUE_FORMAT_V5 => {
                Ok(codec::decode_payload::<QueueConfigV5>(payload)?.into())
            }
            codec::VALUE_FORMAT_V6 | codec::VALUE_FORMAT_V7 => codec::decode_payload(payload),
            _ => unreachable!("split rejects unknown value formats"),
        }
    }

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
        if self.duplicate_detection_history_time_window_millis
            < MIN_DUPLICATE_DETECTION_WINDOW_MILLIS
        {
            return Err(QueueConfigError::DuplicateDetectionWindowTooShort {
                minimum_millis: MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
            });
        }
        if self.duplicate_detection_history_time_window_millis
            > MAX_DUPLICATE_DETECTION_WINDOW_MILLIS
        {
            return Err(QueueConfigError::DuplicateDetectionWindowTooLong {
                maximum_millis: MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
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
    #[error("duplicate detection history must be at least {minimum_millis} milliseconds")]
    DuplicateDetectionWindowTooShort { minimum_millis: u64 },
    #[error("duplicate detection history exceeds the {maximum_millis}-millisecond limit")]
    DuplicateDetectionWindowTooLong { maximum_millis: u64 },
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
    fn duplicate_detection_window_limits_are_inclusive() {
        for window in [
            MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
            MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
        ] {
            let config = QueueConfig {
                requires_duplicate_detection: true,
                duplicate_detection_history_time_window_millis: window,
                ..QueueConfig::default()
            };
            assert_eq!(config.validate(), Ok(config));
        }
        assert_eq!(
            QueueConfig {
                duplicate_detection_history_time_window_millis:
                    MIN_DUPLICATE_DETECTION_WINDOW_MILLIS - 1,
                ..QueueConfig::default()
            }
            .validate(),
            Err(QueueConfigError::DuplicateDetectionWindowTooShort {
                minimum_millis: MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
            })
        );
        assert_eq!(
            QueueConfig {
                duplicate_detection_history_time_window_millis:
                    MAX_DUPLICATE_DETECTION_WINDOW_MILLIS + 1,
                ..QueueConfig::default()
            }
            .validate(),
            Err(QueueConfigError::DuplicateDetectionWindowTooLong {
                maximum_millis: MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
            })
        );
    }

    fn legacy_payload(config: &QueueConfig) -> Vec<u8> {
        postcard::to_stdvec(&(
            config.lock_duration_millis,
            config.max_delivery_count,
            config.default_time_to_live_millis,
            config.max_message_bytes,
            config.requires_session,
        ))
        .expect("configuration encodes")
    }

    #[test]
    fn legacy_configurations_keep_their_settings_with_detection_disabled() -> Result<(), CodecError>
    {
        let original = QueueConfig {
            lock_duration_millis: 20_000,
            max_delivery_count: 3,
            default_time_to_live_millis: Some(500),
            max_message_bytes: 512,
            requires_session: true,
            ..QueueConfig::default()
        };
        for version in codec::VALUE_FORMAT_V1..=codec::VALUE_FORMAT_V5 {
            let mut envelope = vec![version];
            envelope.extend_from_slice(&legacy_payload(&original));
            assert_eq!(QueueConfig::decode(&envelope)?, original);
        }
        Ok(())
    }

    #[test]
    fn configuration_round_trips_in_versions_6_and_7() -> Result<(), CodecError> {
        let original = QueueConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
            ..QueueConfig::default()
        };
        let envelope = codec::encode(&original)?;
        assert_eq!(envelope.first(), Some(&codec::VALUE_FORMAT_V7));
        assert_eq!(QueueConfig::decode(&envelope)?, original);
        let mut old = envelope;
        old[0] = codec::VALUE_FORMAT_V6;
        assert_eq!(QueueConfig::decode(&old)?, original);
        Ok(())
    }

    #[test]
    fn legacy_configuration_bytes_cannot_be_misread_as_version_6() {
        assert_eq!(
            codec::decode_payload::<QueueConfig>(&legacy_payload(&QueueConfig::default())),
            Err(CodecError::Decode)
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
