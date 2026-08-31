use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{CodecError, SessionId, Timestamp, codec};

/// Position of a message in its entity's total order. Allocated from a
/// replicated counter, never from a local generator.
#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Identifies one delivery's claim on a message.
///
/// The token is drawn from a replicated per-entity counter so that every
/// replica derives the same value while applying the same command. It is not a
/// secret: the protocol edge is responsible for only handing a token to the
/// receiver that acquired it.
#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct LockToken(u64);

impl LockToken {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for LockToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiveMode {
    PeekLock,
    ReceiveAndDelete,
}

impl ReceiveMode {
    pub const fn delivery_guarantee(self) -> DeliveryGuarantee {
        match self {
            Self::PeekLock => DeliveryGuarantee::AtLeastOnce,
            Self::ReceiveAndDelete => DeliveryGuarantee::AtMostOnce,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryGuarantee {
    AtLeastOnce,
    AtMostOnce,
}

/// Broker-visible state carried with a delivery or browse result.
///
/// A deferred message is temporarily locked while it is received by sequence
/// number. Persisting its origin in that lock is what makes abandon and lock
/// expiry put it back in the deferred set instead of making it visible to an
/// ordinary receiver. `Scheduled` is browse-only: a scheduled placeholder is
/// never locked or handed to a receiver.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryOrigin {
    Ready,
    Deferred,
    /// Browse-only origin for a message that has not reached its scheduled
    /// enqueue time. Scheduled messages never carry delivery locks.
    Scheduled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeadLetterReason {
    MaxDeliveryCountExceeded,
    TimeToLiveExpired,
    Application(String),
}

impl DeadLetterReason {
    /// The reason string the Service Bus SDKs expect to read back from a
    /// dead-lettered message.
    pub fn as_str(&self) -> &str {
        match self {
            Self::MaxDeliveryCountExceeded => "MaxDeliveryCountExceeded",
            Self::TimeToLiveExpired => "TTLExpiredException",
            Self::Application(reason) => reason,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeadLetterInfo {
    pub reason: DeadLetterReason,
    pub description: String,
    pub dead_lettered_at: Timestamp,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageState {
    Ready,
    Deferred,
    Scheduled,
    Locked {
        token: LockToken,
        locked_until: Timestamp,
        origin: DeliveryOrigin,
    },
}

/// The protocol-native representation of a message.
///
/// The broker treats these bytes as opaque durable data. Protocol adapters
/// retain normalized fields alongside them for routing, expiry, and settlement
/// decisions, then use the envelope to reconstruct the message without losing
/// protocol-specific metadata or body forms.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageEnvelope(Vec<u8>);

impl MessageEnvelope {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<[u8]> for MessageEnvelope {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for MessageEnvelope {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageRecord {
    pub sequence: SequenceNumber,
    pub message_id: String,
    pub body: Vec<u8>,
    pub enqueued_at: Timestamp,
    /// Requested enqueue time for a scheduled send. This survives activation
    /// for protocol observability while `enqueued_at` is replaced with the
    /// actual activation time.
    pub scheduled_enqueue_at: Option<Timestamp>,
    /// Active messages expire at this deadline. For a scheduled placeholder,
    /// this temporarily records scheduled time plus effective TTL so activation
    /// can preserve the duration while rebasing it on the actual enqueue time.
    pub expires_at: Option<Timestamp>,
    pub delivery_count: u32,
    pub state: MessageState,
    /// Set exactly when the queue requires sessions.
    pub session_id: Option<SessionId>,
    /// Why the message was dead-lettered, once it lives in a dead-letter queue.
    ///
    /// Dead-lettered is not a state of its own: a message in a dead-letter
    /// queue is ready or locked like any other, which is what lets the same
    /// receive and settlement machinery drain it. This field is what remembers
    /// how it got there.
    pub dead_letter: Option<DeadLetterInfo>,
    /// The lossless protocol-native message, when the ingress adapter supplied
    /// one. Non-protocol producers have no envelope.
    pub envelope: Option<MessageEnvelope>,
}

impl MessageRecord {
    /// Decodes a stored message from the V1 value format.
    pub fn decode(envelope: &[u8]) -> Result<Self, CodecError> {
        codec::decode(envelope)
    }

    /// A message with no configured lifetime never expires, which is the
    /// Service Bus default.
    pub fn is_expired_at(&self, now: Timestamp) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }

    pub fn dead_letter_info(&self) -> Option<&DeadLetterInfo> {
        self.dead_letter.as_ref()
    }
}

/// One message handed to a receiver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivery {
    pub sequence: SequenceNumber,
    pub message_id: String,
    pub body: Vec<u8>,
    pub enqueued_at: Timestamp,
    pub scheduled_enqueue_at: Option<Timestamp>,
    pub expires_at: Option<Timestamp>,
    pub delivery_count: u32,
    /// Whether this was active, deferred, or a browse-only scheduled result.
    pub origin: DeliveryOrigin,
    /// Absent in receive-and-delete, where the message is already gone.
    pub lock: Option<DeliveryLock>,
    /// The session this message was delivered from, on a session queue.
    pub session_id: Option<SessionId>,
    /// Why the message was dead-lettered, when it came from a dead-letter
    /// queue.
    pub dead_letter: Option<DeadLetterInfo>,
    /// The lossless protocol-native message, when one accompanied the send.
    pub envelope: Option<MessageEnvelope>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryLock {
    pub token: LockToken,
    pub locked_until: Timestamp,
    /// Effective duration used to create this lock, after applying any
    /// per-receive override.
    pub lock_duration_millis: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(expires_at: Option<Timestamp>) -> MessageRecord {
        MessageRecord {
            sequence: SequenceNumber::new(1),
            message_id: String::from("m-1"),
            body: Vec::new(),
            enqueued_at: Timestamp::from_millis(0),
            scheduled_enqueue_at: None,
            expires_at,
            delivery_count: 0,
            state: MessageState::Ready,
            session_id: None,
            dead_letter: None,
            envelope: None,
        }
    }

    #[test]
    fn a_message_without_a_lifetime_never_expires() {
        assert!(!record(None).is_expired_at(Timestamp::from_millis(u64::MAX)));
    }

    #[test]
    fn expiry_is_inclusive_of_the_deadline() {
        let record = record(Some(Timestamp::from_millis(100)));
        assert!(!record.is_expired_at(Timestamp::from_millis(99)));
        assert!(record.is_expired_at(Timestamp::from_millis(100)));
    }

    #[test]
    fn a_message_round_trips_through_version_one() -> Result<(), CodecError> {
        let original = MessageRecord {
            session_id: Some(SessionId::new("cart-1").expect("a valid session id")),
            ..record(None)
        };
        let envelope = codec::encode(&original)?;
        assert_eq!(envelope.first(), Some(&codec::VALUE_FORMAT_V1));
        assert_eq!(MessageRecord::decode(&envelope)?, original);
        Ok(())
    }

    #[test]
    fn a_protocol_envelope_round_trips_through_version_one() -> Result<(), CodecError> {
        let original = MessageRecord {
            envelope: Some(MessageEnvelope::new(vec![
                0, 0x53, 0x77, 0xa1, 3, b'a', b'm', b'q',
            ])),
            ..record(None)
        };
        let encoded = codec::encode(&original)?;

        assert_eq!(encoded.first(), Some(&codec::VALUE_FORMAT_V1));
        assert_eq!(MessageRecord::decode(&encoded)?, original);
        Ok(())
    }

    #[test]
    fn a_newer_message_format_is_rejected() {
        assert_eq!(
            MessageRecord::decode(&[codec::VALUE_FORMAT_V1 + 1, 0]),
            Err(CodecError::UnsupportedVersion {
                version: codec::VALUE_FORMAT_V1 + 1,
            })
        );
    }

    #[test]
    fn receive_modes_map_to_their_delivery_guarantees() {
        assert_eq!(
            ReceiveMode::PeekLock.delivery_guarantee(),
            DeliveryGuarantee::AtLeastOnce
        );
        assert_eq!(
            ReceiveMode::ReceiveAndDelete.delivery_guarantee(),
            DeliveryGuarantee::AtMostOnce
        );
    }
}
