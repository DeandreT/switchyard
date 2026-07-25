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
    Locked {
        token: LockToken,
        locked_until: Timestamp,
    },
    DeadLettered(DeadLetterInfo),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageRecord {
    pub sequence: SequenceNumber,
    pub message_id: String,
    pub body: Vec<u8>,
    pub enqueued_at: Timestamp,
    pub expires_at: Option<Timestamp>,
    pub delivery_count: u32,
    pub state: MessageState,
    /// Set exactly when the queue requires sessions. `session_id` is last in the
    /// record so that a version 1 payload is a strict prefix of a version 2 one,
    /// which makes decoding a version 1 record as version 2 run off the end of
    /// the buffer instead of silently producing a different message.
    pub session_id: Option<SessionId>,
}

/// The version 1 shape of [`MessageRecord`], from before queues had sessions.
///
/// Kept so that a store written by an earlier build still reads. Version 1
/// predates sessions entirely, so every message it holds belongs to no session.
#[derive(Deserialize)]
struct MessageRecordV1 {
    sequence: SequenceNumber,
    message_id: String,
    body: Vec<u8>,
    enqueued_at: Timestamp,
    expires_at: Option<Timestamp>,
    delivery_count: u32,
    state: MessageState,
}

impl From<MessageRecordV1> for MessageRecord {
    fn from(record: MessageRecordV1) -> Self {
        Self {
            sequence: record.sequence,
            message_id: record.message_id,
            body: record.body,
            enqueued_at: record.enqueued_at,
            expires_at: record.expires_at,
            delivery_count: record.delivery_count,
            state: record.state,
            session_id: None,
        }
    }
}

impl MessageRecord {
    /// Decodes a stored message, migrating a version 1 record on the way.
    ///
    /// Messages are the one record whose shape changed, so they decode through
    /// here rather than through the shape-stable [`codec::decode`].
    pub fn decode(envelope: &[u8]) -> Result<Self, CodecError> {
        let (version, payload) = codec::split(envelope)?;
        match version {
            codec::VALUE_FORMAT_V1 => Ok(codec::decode_payload::<MessageRecordV1>(payload)?.into()),
            _ => codec::decode_payload(payload),
        }
    }

    /// A message with no configured lifetime never expires, which is the
    /// Service Bus default.
    pub fn is_expired_at(&self, now: Timestamp) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }

    pub fn dead_letter_info(&self) -> Option<&DeadLetterInfo> {
        match &self.state {
            MessageState::DeadLettered(info) => Some(info),
            _ => None,
        }
    }
}

/// One message handed to a receiver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivery {
    pub sequence: SequenceNumber,
    pub message_id: String,
    pub body: Vec<u8>,
    pub enqueued_at: Timestamp,
    pub delivery_count: u32,
    /// Absent in receive-and-delete, where the message is already gone.
    pub lock: Option<DeliveryLock>,
    /// The session this message was delivered from, on a session queue.
    pub session_id: Option<SessionId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryLock {
    pub token: LockToken,
    pub locked_until: Timestamp,
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
            expires_at,
            delivery_count: 0,
            state: MessageState::Ready,
            session_id: None,
        }
    }

    /// The version 1 payload of [`record`], which is every field except the
    /// session identifier version 2 appended.
    fn version_1_payload() -> Vec<u8> {
        postcard::to_stdvec(&(
            SequenceNumber::new(1),
            String::from("m-1"),
            Vec::<u8>::new(),
            Timestamp::from_millis(0),
            Option::<Timestamp>::None,
            0_u32,
            MessageState::Ready,
        ))
        .expect("a message encodes")
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
    fn a_message_round_trips_through_the_active_format() -> Result<(), CodecError> {
        let original = MessageRecord {
            session_id: Some(SessionId::new("cart-1").expect("a valid session id")),
            ..record(None)
        };
        let envelope = codec::encode(&original)?;
        assert_eq!(envelope.first(), Some(&codec::VALUE_FORMAT_V2));
        assert_eq!(MessageRecord::decode(&envelope)?, original);
        Ok(())
    }

    #[test]
    fn a_version_1_message_reads_as_belonging_to_no_session() -> Result<(), CodecError> {
        let mut envelope = vec![codec::VALUE_FORMAT_V1];
        envelope.extend_from_slice(&version_1_payload());

        // Version 1 predates sessions, so every message it holds is session-less.
        assert_eq!(MessageRecord::decode(&envelope)?, record(None));
        Ok(())
    }

    #[test]
    fn version_1_bytes_cannot_be_misread_as_the_active_format() {
        // The rollback direction. `session_id` is the last field, so a version 1
        // payload runs off the end of the buffer when it is read as version 2
        // rather than decoding into some other message.
        assert_eq!(
            codec::decode_payload::<MessageRecord>(&version_1_payload()),
            Err(CodecError::Decode)
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
