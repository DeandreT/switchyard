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
    /// Set exactly when the queue requires sessions.
    ///
    /// Fields are appended in the order the record grew — `session_id` in
    /// version 2, `dead_letter` in version 3 — so an older payload is a strict
    /// prefix of a newer one, and reading it as the newer version runs off the
    /// end of the buffer instead of silently producing a different message.
    pub session_id: Option<SessionId>,
    /// Why the message was dead-lettered, once it lives in a dead-letter queue.
    ///
    /// Dead-lettered is not a state of its own: a message in a dead-letter
    /// queue is ready or locked like any other, which is what lets the same
    /// receive and settlement machinery drain it. This field is what remembers
    /// how it got there.
    pub dead_letter: Option<DeadLetterInfo>,
}

/// The state enum as versions 1 and 2 stored it, when dead-lettered was a
/// state rather than a queue.
#[derive(Deserialize)]
enum MessageStateV2 {
    Ready,
    Locked {
        token: LockToken,
        locked_until: Timestamp,
    },
    DeadLettered(DeadLetterInfo),
}

impl From<MessageStateV2> for (MessageState, Option<DeadLetterInfo>) {
    fn from(state: MessageStateV2) -> Self {
        match state {
            MessageStateV2::Ready => (MessageState::Ready, None),
            MessageStateV2::Locked {
                token,
                locked_until,
            } => (
                MessageState::Locked {
                    token,
                    locked_until,
                },
                None,
            ),
            MessageStateV2::DeadLettered(info) => (MessageState::Ready, Some(info)),
        }
    }
}

/// The version 1 shape of [`MessageRecord`], from before queues had sessions.
#[derive(Deserialize)]
struct MessageRecordV1 {
    sequence: SequenceNumber,
    message_id: String,
    body: Vec<u8>,
    enqueued_at: Timestamp,
    expires_at: Option<Timestamp>,
    delivery_count: u32,
    state: MessageStateV2,
}

/// The version 2 shape, from before the dead-letter queue was a queue.
#[derive(Deserialize)]
struct MessageRecordV2 {
    sequence: SequenceNumber,
    message_id: String,
    body: Vec<u8>,
    enqueued_at: Timestamp,
    expires_at: Option<Timestamp>,
    delivery_count: u32,
    state: MessageStateV2,
    session_id: Option<SessionId>,
}

impl From<MessageRecordV2> for MessageRecord {
    fn from(record: MessageRecordV2) -> Self {
        let (state, dead_letter) = record.state.into();
        Self {
            sequence: record.sequence,
            message_id: record.message_id,
            body: record.body,
            enqueued_at: record.enqueued_at,
            expires_at: record.expires_at,
            delivery_count: record.delivery_count,
            state,
            session_id: record.session_id,
            dead_letter,
        }
    }
}

impl From<MessageRecordV1> for MessageRecord {
    fn from(record: MessageRecordV1) -> Self {
        MessageRecordV2 {
            sequence: record.sequence,
            message_id: record.message_id,
            body: record.body,
            enqueued_at: record.enqueued_at,
            expires_at: record.expires_at,
            delivery_count: record.delivery_count,
            state: record.state,
            session_id: None,
        }
        .into()
    }
}

impl MessageRecord {
    /// Decodes a stored message, migrating an older record on the way.
    ///
    /// Messages are the one record whose shape has changed, so they decode
    /// through here rather than through the shape-stable [`codec::decode`].
    pub fn decode(envelope: &[u8]) -> Result<Self, CodecError> {
        let (version, payload) = codec::split(envelope)?;
        match version {
            codec::VALUE_FORMAT_V1 => Ok(codec::decode_payload::<MessageRecordV1>(payload)?.into()),
            codec::VALUE_FORMAT_V2 => Ok(codec::decode_payload::<MessageRecordV2>(payload)?.into()),
            _ => codec::decode_payload(payload),
        }
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
            dead_letter: None,
        }
    }

    /// The state enum as versions 1 and 2 wrote it, for building old payloads.
    #[derive(Serialize)]
    enum StoredStateV2 {
        Ready,
        #[expect(dead_code)]
        Locked {
            token: LockToken,
            locked_until: Timestamp,
        },
        DeadLettered(DeadLetterInfo),
    }

    /// The version 1 payload of [`record`]: every field up to the session
    /// identifier version 2 appended.
    fn version_1_payload(state: StoredStateV2) -> Vec<u8> {
        postcard::to_stdvec(&(
            SequenceNumber::new(1),
            String::from("m-1"),
            Vec::<u8>::new(),
            Timestamp::from_millis(0),
            Option::<Timestamp>::None,
            0_u32,
            state,
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
        assert_eq!(envelope.first(), Some(&codec::VALUE_FORMAT_V3));
        assert_eq!(MessageRecord::decode(&envelope)?, original);
        Ok(())
    }

    #[test]
    fn a_version_1_message_reads_as_belonging_to_no_session() -> Result<(), CodecError> {
        let mut envelope = vec![codec::VALUE_FORMAT_V1];
        envelope.extend_from_slice(&version_1_payload(StoredStateV2::Ready));

        // Version 1 predates sessions, so every message it holds is session-less.
        assert_eq!(MessageRecord::decode(&envelope)?, record(None));
        Ok(())
    }

    #[test]
    fn an_old_dead_lettered_state_reads_as_a_ready_dead_letter() -> Result<(), CodecError> {
        // Versions 1 and 2 stored dead-lettered as a state. It reads back as a
        // ready message that remembers why it was dead-lettered, which is the
        // shape a dead-letter queue drains.
        let info = DeadLetterInfo {
            reason: DeadLetterReason::TimeToLiveExpired,
            description: String::from("expired"),
            dead_lettered_at: Timestamp::from_millis(9),
        };
        let mut envelope = vec![codec::VALUE_FORMAT_V1];
        envelope.extend_from_slice(&version_1_payload(StoredStateV2::DeadLettered(
            info.clone(),
        )));

        let decoded = MessageRecord::decode(&envelope)?;
        assert_eq!(decoded.state, MessageState::Ready);
        assert_eq!(decoded.dead_letter, Some(info));
        Ok(())
    }

    #[test]
    fn old_bytes_cannot_be_misread_as_the_active_format() {
        // The rollback direction. Fields are appended in version order, so an
        // older payload runs off the end of the buffer when read as the active
        // version rather than decoding into some other message.
        assert_eq!(
            codec::decode_payload::<MessageRecord>(&version_1_payload(StoredStateV2::Ready)),
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
