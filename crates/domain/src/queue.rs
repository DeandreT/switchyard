use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BrokerError, CodecError, LockToken, SequenceNumber, codec};

/// Longest lock Service Bus accepts, and the value Switchyard enforces so that
/// a client cannot pin a message indefinitely.
pub const MAX_LOCK_DURATION_MILLIS: u64 = 5 * 60 * 1_000;
pub const DEFAULT_LOCK_DURATION_MILLIS: u64 = 60 * 1_000;
pub const DEFAULT_MAX_DELIVERY_COUNT: u32 = 10;
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 256 * 1024;
pub const MIN_DUPLICATE_DETECTION_WINDOW_MILLIS: u64 = 20 * 1_000;
pub const DEFAULT_DUPLICATE_DETECTION_WINDOW_MILLIS: u64 = 10 * 60 * 1_000;
pub const MAX_DUPLICATE_DETECTION_WINDOW_MILLIS: u64 = 7 * 24 * 60 * 60 * 1_000;
/// Largest allocatable sequence, preserving its exact nonnegative Int64 wire
/// representation. Exhaustion refuses new allocations rather than rolling over.
pub const MAX_SEQUENCE_NUMBER: u64 = i64::MAX as u64;

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
    /// Expired messages are dropped by default. Enabling this moves them to
    /// the dead-letter queue with the TTLExpiredException reason instead.
    pub dead_lettering_on_message_expiration: bool,
}

/// An explicit lifetime replacement. An absent patch field means unchanged;
/// `Unlimited` clears a finite default in both JSON and replicated commands.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueTimeToLiveUpdate {
    Unlimited,
    Finite { millis: u64 },
}

/// A partial replacement of queue settings. Creation-only properties may be
/// restated with the same value, but cannot be enabled or disabled afterward.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueConfigUpdate {
    pub lock_duration_millis: Option<u64>,
    pub max_delivery_count: Option<u32>,
    pub default_time_to_live_millis: Option<QueueTimeToLiveUpdate>,
    pub max_message_bytes: Option<usize>,
    pub requires_session: Option<bool>,
    pub requires_duplicate_detection: Option<bool>,
    pub duplicate_detection_history_time_window_millis: Option<u64>,
    pub dead_lettering_on_message_expiration: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueImmutableProperty {
    RequiresSession,
    RequiresDuplicateDetection,
}

impl std::fmt::Display for QueueImmutableProperty {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RequiresSession => "requires_session",
            Self::RequiresDuplicateDetection => "requires_duplicate_detection",
        })
    }
}

impl QueueConfigUpdate {
    pub(crate) fn apply_to(self, current: QueueConfig) -> Result<QueueConfig, BrokerError> {
        if self
            .requires_session
            .is_some_and(|value| value != current.requires_session)
        {
            return Err(BrokerError::QueuePropertyIsImmutable {
                property: QueueImmutableProperty::RequiresSession,
            });
        }
        if self
            .requires_duplicate_detection
            .is_some_and(|value| value != current.requires_duplicate_detection)
        {
            return Err(BrokerError::QueuePropertyIsImmutable {
                property: QueueImmutableProperty::RequiresDuplicateDetection,
            });
        }
        Ok(QueueConfig {
            lock_duration_millis: self
                .lock_duration_millis
                .unwrap_or(current.lock_duration_millis),
            max_delivery_count: self
                .max_delivery_count
                .unwrap_or(current.max_delivery_count),
            default_time_to_live_millis: match self.default_time_to_live_millis {
                None => current.default_time_to_live_millis,
                Some(QueueTimeToLiveUpdate::Unlimited) => None,
                Some(QueueTimeToLiveUpdate::Finite { millis }) => Some(millis),
            },
            max_message_bytes: self.max_message_bytes.unwrap_or(current.max_message_bytes),
            duplicate_detection_history_time_window_millis: self
                .duplicate_detection_history_time_window_millis
                .unwrap_or(current.duplicate_detection_history_time_window_millis),
            dead_lettering_on_message_expiration: self
                .dead_lettering_on_message_expiration
                .unwrap_or(current.dead_lettering_on_message_expiration),
            ..current
        }
        .validate()?)
    }
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
            dead_lettering_on_message_expiration: true,
            ..Self::default()
        }
    }
}

/// Versions 6 and 7, which always dead-lettered expired messages.
#[derive(Deserialize)]
struct QueueConfigV7 {
    lock_duration_millis: u64,
    max_delivery_count: u32,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: usize,
    requires_session: bool,
    requires_duplicate_detection: bool,
    duplicate_detection_history_time_window_millis: u64,
}

impl From<QueueConfigV7> for QueueConfig {
    fn from(config: QueueConfigV7) -> Self {
        Self {
            lock_duration_millis: config.lock_duration_millis,
            max_delivery_count: config.max_delivery_count,
            default_time_to_live_millis: config.default_time_to_live_millis,
            max_message_bytes: config.max_message_bytes,
            requires_session: config.requires_session,
            requires_duplicate_detection: config.requires_duplicate_detection,
            duplicate_detection_history_time_window_millis: config
                .duplicate_detection_history_time_window_millis,
            dead_lettering_on_message_expiration: true,
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
            dead_lettering_on_message_expiration: false,
        }
    }
}

impl QueueConfig {
    /// The receive-only shadow policy used for creation and topology validation.
    pub fn dead_letter_shadow(self) -> Self {
        Self {
            max_delivery_count: u32::MAX,
            default_time_to_live_millis: None,
            requires_session: false,
            requires_duplicate_detection: false,
            dead_lettering_on_message_expiration: false,
            ..self
        }
    }

    /// Decodes a stored configuration, preserving older settings and keeping
    /// duplicate detection disabled on queues that predate the feature. Legacy
    /// queues retain their always-dead-letter expiration policy.
    pub fn decode(envelope: &[u8]) -> Result<Self, CodecError> {
        let (version, payload) = codec::split(envelope)?;
        match version {
            codec::VALUE_FORMAT_V1..=codec::VALUE_FORMAT_V5 => {
                Ok(codec::decode_payload::<QueueConfigV5>(payload)?.into())
            }
            codec::VALUE_FORMAT_V6 | codec::VALUE_FORMAT_V7 => {
                Ok(codec::decode_payload::<QueueConfigV7>(payload)?.into())
            }
            codec::VALUE_FORMAT_V8 | codec::VALUE_FORMAT_V9 | codec::VALUE_FORMAT_V10 => {
                codec::decode_payload(payload)
            }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueCounterKind {
    Sequence,
    LockToken,
}

impl std::fmt::Display for QueueCounterKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Sequence => "sequence number",
            Self::LockToken => "lock token",
        })
    }
}

impl QueueCounters {
    pub(crate) fn allocate_sequence(&mut self) -> Result<SequenceNumber, BrokerError> {
        if self.next_sequence > MAX_SEQUENCE_NUMBER {
            return Err(BrokerError::QueueCounterExhausted {
                counter: QueueCounterKind::Sequence,
            });
        }
        let sequence = SequenceNumber::new(self.next_sequence);
        self.next_sequence += 1;
        Ok(sequence)
    }

    pub(crate) fn allocate_lock_token(&mut self) -> Result<LockToken, BrokerError> {
        let next =
            self.next_lock_token
                .checked_add(1)
                .ok_or(BrokerError::QueueCounterExhausted {
                    counter: QueueCounterKind::LockToken,
                })?;
        let token = LockToken::new(self.next_lock_token);
        self.next_lock_token = next;
        Ok(token)
    }
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
        assert!(!QueueConfig::default().dead_lettering_on_message_expiration);
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
            dead_lettering_on_message_expiration: true,
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
    fn configurations_from_versions_6_and_7_preserve_expiry_behavior() -> Result<(), CodecError> {
        let original = QueueConfig {
            requires_duplicate_detection: true,
            duplicate_detection_history_time_window_millis: MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        };
        let payload = version_7_payload(&original);
        for version in [codec::VALUE_FORMAT_V6, codec::VALUE_FORMAT_V7] {
            let mut envelope = vec![version];
            envelope.extend_from_slice(&payload);
            assert_eq!(QueueConfig::decode(&envelope)?, original);
        }
        Ok(())
    }

    fn version_7_payload(config: &QueueConfig) -> Vec<u8> {
        postcard::to_stdvec(&(
            config.lock_duration_millis,
            config.max_delivery_count,
            config.default_time_to_live_millis,
            config.max_message_bytes,
            config.requires_session,
            config.requires_duplicate_detection,
            config.duplicate_detection_history_time_window_millis,
        ))
        .expect("configuration encodes")
    }

    #[test]
    fn current_configurations_round_trip_both_expiry_policies() -> Result<(), CodecError> {
        for policy in [false, true] {
            let config = QueueConfig {
                dead_lettering_on_message_expiration: policy,
                ..QueueConfig::default()
            };
            let envelope = codec::encode(&config)?;
            assert_eq!(envelope.first(), Some(&codec::ACTIVE_VALUE_FORMAT));
            assert_eq!(QueueConfig::decode(&envelope)?, config);
            let mut previous = envelope;
            previous[0] = codec::VALUE_FORMAT_V8;
            assert_eq!(QueueConfig::decode(&previous)?, config);
        }
        Ok(())
    }

    #[test]
    fn expiry_policy_cannot_be_silently_misread_on_rollback() -> Result<(), CodecError> {
        let config = QueueConfig::default();
        let mut current = codec::encode(&config)?;
        for version in [codec::VALUE_FORMAT_V6, codec::VALUE_FORMAT_V7] {
            current[0] = version;
            assert_eq!(QueueConfig::decode(&current), Err(CodecError::Decode));
        }
        let mut old = vec![codec::VALUE_FORMAT_V8];
        old.extend_from_slice(&version_7_payload(&config));
        assert_eq!(QueueConfig::decode(&old), Err(CodecError::Decode));
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

    #[test]
    fn sequence_allocation_preserves_the_last_signed_value_then_refuses() {
        let mut counters = QueueCounters {
            next_sequence: MAX_SEQUENCE_NUMBER,
            next_lock_token: 42,
        };
        assert_eq!(
            counters.allocate_sequence(),
            Ok(SequenceNumber::new(MAX_SEQUENCE_NUMBER))
        );
        assert_eq!(counters.next_sequence, MAX_SEQUENCE_NUMBER + 1);
        assert_eq!(counters.next_lock_token, 42);
        let exhausted = counters;
        for _ in 0..2 {
            assert_eq!(
                counters.allocate_sequence(),
                Err(BrokerError::QueueCounterExhausted {
                    counter: QueueCounterKind::Sequence,
                })
            );
            assert_eq!(counters, exhausted);
        }
    }

    #[test]
    fn legacy_unsigned_sequence_counters_are_refused_without_mutation() {
        for next_sequence in [MAX_SEQUENCE_NUMBER + 1, u64::MAX] {
            let mut counters = QueueCounters {
                next_sequence,
                next_lock_token: 9,
            };
            let before = counters;
            assert_eq!(
                counters.allocate_sequence(),
                Err(BrokerError::QueueCounterExhausted {
                    counter: QueueCounterKind::Sequence,
                })
            );
            assert_eq!(counters, before);
        }
    }

    #[test]
    fn lock_allocation_reserves_an_exhausted_sentinel_without_aliasing() {
        let mut counters = QueueCounters {
            next_sequence: 123,
            next_lock_token: u64::MAX - 1,
        };
        assert_eq!(
            counters.allocate_lock_token(),
            Ok(LockToken::new(u64::MAX - 1))
        );
        assert_eq!(counters.next_lock_token, u64::MAX);
        assert_eq!(counters.next_sequence, 123);
        let exhausted = counters;
        for _ in 0..2 {
            assert_eq!(
                counters.allocate_lock_token(),
                Err(BrokerError::QueueCounterExhausted {
                    counter: QueueCounterKind::LockToken,
                })
            );
            assert_eq!(counters, exhausted);
        }
    }

    #[test]
    fn counter_storage_shape_remains_two_unsigned_fields() -> Result<(), CodecError> {
        let counters = QueueCounters {
            next_sequence: 1,
            next_lock_token: 2,
        };
        let mut encoded = codec::encode(&counters)?;
        assert_eq!(encoded, vec![codec::ACTIVE_VALUE_FORMAT, 1, 2]);
        for version in codec::VALUE_FORMAT_V1..=codec::ACTIVE_VALUE_FORMAT {
            encoded[0] = version;
            assert_eq!(codec::decode::<QueueCounters>(&encoded)?, counters);
        }
        Ok(())
    }
}
