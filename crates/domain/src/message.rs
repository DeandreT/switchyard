use std::fmt;

use serde::{Deserialize, Serialize};

use crate::Timestamp;

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
}

impl MessageRecord {
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryLock {
    pub token: LockToken,
    pub locked_until: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_without_a_lifetime_never_expires() {
        let record = MessageRecord {
            sequence: SequenceNumber::new(1),
            message_id: String::from("m-1"),
            body: Vec::new(),
            enqueued_at: Timestamp::from_millis(0),
            expires_at: None,
            delivery_count: 0,
            state: MessageState::Ready,
        };
        assert!(!record.is_expired_at(Timestamp::from_millis(u64::MAX)));
    }

    #[test]
    fn expiry_is_inclusive_of_the_deadline() {
        let record = MessageRecord {
            sequence: SequenceNumber::new(1),
            message_id: String::from("m-1"),
            body: Vec::new(),
            enqueued_at: Timestamp::from_millis(0),
            expires_at: Some(Timestamp::from_millis(100)),
            delivery_count: 0,
            state: MessageState::Ready,
        };
        assert!(!record.is_expired_at(Timestamp::from_millis(99)));
        assert!(record.is_expired_at(Timestamp::from_millis(100)));
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
