//! Moving a message across the wire boundary.
//!
//! An AMQP message carries far more than the broker stores. What crosses in is
//! the body, the identifier a client can correlate on, the session it belongs
//! to, and how long it may live; everything else stays on the wire. What crosses
//! back out is what the broker recorded, so a redelivery looks the same as the
//! first attempt.

use amqp::{ApplicationProperties, Body, Message, MessageId, Properties};
use domain::{Delivery, SessionId};

use crate::{ProtocolError, parse_session_id};

/// What a client sent, reduced to what the broker keeps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingMessage {
    pub message_id: String,
    pub body: Vec<u8>,
    pub session_id: Option<SessionId>,
    pub time_to_live_millis: Option<u64>,
}

/// Reads an incoming AMQP message into the parts a send command needs.
///
/// A message with no identifier of its own is accepted: Service Bus assigns one
/// rather than refusing the send, and the broker's own sequence number is what
/// actually identifies the message afterwards.
pub fn read_incoming(message: &Message) -> Result<IncomingMessage, ProtocolError> {
    let properties = message.properties.as_ref();
    let session_id = properties
        .and_then(|properties| properties.group_id.as_deref())
        .map(parse_session_id)
        .transpose()?;

    Ok(IncomingMessage {
        message_id: properties
            .and_then(|properties| properties.message_id.as_ref())
            .map(message_id_text)
            .unwrap_or_default(),
        body: body_bytes(&message.body),
        session_id,
        // The AMQP header carries a lifetime in milliseconds already.
        time_to_live_millis: message
            .header
            .as_ref()
            .and_then(|header| header.ttl)
            .map(u64::from),
    })
}

/// The application property Service Bus clients read a dead-letter reason from.
pub const DEAD_LETTER_REASON_PROPERTY: &str = "DeadLetterReason";
/// The application property carrying the dead-letter description.
pub const DEAD_LETTER_DESCRIPTION_PROPERTY: &str = "DeadLetterErrorDescription";

/// Builds the message handed back to a receiving client.
pub fn write_delivery(delivery: &Delivery) -> Message {
    let properties = Properties {
        message_id: Some(delivery.message_id.clone().into()),
        group_id: delivery
            .session_id
            .as_ref()
            .map(|session_id| session_id.as_str().to_owned()),
        ..Properties::default()
    };

    let mut message = Message::data(delivery.body.clone());
    message.properties = Some(properties);

    // A message drained from a dead-letter queue says why it is there, in the
    // properties the Service Bus SDKs read.
    if let Some(dead_letter) = &delivery.dead_letter {
        let mut properties = ApplicationProperties::default();
        properties.insert(
            DEAD_LETTER_REASON_PROPERTY,
            dead_letter.reason.as_str().to_owned(),
        );
        properties.insert(
            DEAD_LETTER_DESCRIPTION_PROPERTY,
            dead_letter.description.clone(),
        );
        message.application_properties = Some(properties);
    }
    message
}

/// The bytes a body carries, whatever shape it arrived in.
///
/// A value or an empty body is not an error: the broker stores bodies opaquely
/// and a client is entitled to send nothing.
fn body_bytes(body: &Body) -> Vec<u8> {
    match body {
        // A body may arrive as several data sections; the broker stores the
        // payload as one opaque run of bytes.
        Body::Data(sections) => sections
            .iter()
            .flat_map(|section| section.iter().copied())
            .collect(),
        Body::Sequence(_) | Body::Value(_) | Body::Empty => Vec::new(),
    }
}

fn message_id_text(message_id: &MessageId) -> String {
    match message_id {
        MessageId::String(text) => text.to_string(),
        MessageId::Ulong(value) => value.to_string(),
        MessageId::Uuid(value) => format!("{value:?}"),
        MessageId::Binary(value) => value.iter().map(|byte| format!("{byte:02x}")).collect(),
    }
}

#[cfg(test)]
mod tests {
    use domain::{DeliveryLock, LockToken, SequenceNumber, Timestamp};

    use super::*;

    fn sent(properties: Option<Properties>, body: Vec<u8>) -> Message {
        let mut message = Message::data(body);
        message.properties = properties;
        message
    }

    #[test]
    fn a_body_and_identifier_cross_in() -> Result<(), ProtocolError> {
        let properties = Properties {
            message_id: Some(String::from("order-1").into()),
            ..Properties::default()
        };
        let incoming = read_incoming(&sent(Some(properties), b"payload".to_vec()))?;

        assert_eq!(incoming.message_id, "order-1");
        assert_eq!(incoming.body, b"payload".to_vec());
        assert_eq!(incoming.session_id, None);
        Ok(())
    }

    #[test]
    fn a_group_id_is_the_session_the_message_belongs_to() -> Result<(), ProtocolError> {
        let properties = Properties {
            group_id: Some(String::from("cart-1")),
            ..Properties::default()
        };
        let incoming = read_incoming(&sent(Some(properties), Vec::new()))?;

        assert_eq!(
            incoming.session_id.as_ref().map(SessionId::as_str),
            Some("cart-1")
        );
        Ok(())
    }

    #[test]
    fn a_session_the_broker_cannot_key_on_is_refused_at_the_edge() {
        let properties = Properties {
            group_id: Some(String::from("cart\u{0}1")),
            ..Properties::default()
        };
        assert!(matches!(
            read_incoming(&sent(Some(properties), Vec::new())),
            Err(ProtocolError::InvalidSessionId { .. })
        ));
    }

    #[test]
    fn a_message_without_properties_still_crosses() -> Result<(), ProtocolError> {
        // Service Bus assigns an identifier rather than refusing the send, and
        // the sequence number is what identifies the message afterwards.
        let incoming = read_incoming(&sent(None, b"payload".to_vec()))?;
        assert_eq!(incoming.message_id, "");
        assert_eq!(incoming.body, b"payload".to_vec());
        Ok(())
    }

    #[test]
    fn a_delivery_carries_its_identifier_and_session_back_out() -> Result<(), ProtocolError> {
        let delivery = Delivery {
            sequence: SequenceNumber::new(7),
            message_id: String::from("order-1"),
            body: b"payload".to_vec(),
            enqueued_at: Timestamp::from_millis(10),
            delivery_count: 1,
            lock: Some(DeliveryLock {
                token: LockToken::new(1),
                locked_until: Timestamp::from_millis(100),
            }),
            session_id: Some(SessionId::new("cart-1").expect("a valid session id")),
            dead_letter: None,
        };

        // A round trip through the wire shape keeps what the broker recorded, so
        // a redelivery looks like the first attempt.
        let incoming = read_incoming(&write_delivery(&delivery))?;
        assert_eq!(incoming.message_id, "order-1");
        assert_eq!(incoming.body, b"payload".to_vec());
        assert_eq!(
            incoming.session_id.as_ref().map(SessionId::as_str),
            Some("cart-1")
        );
        Ok(())
    }
}
