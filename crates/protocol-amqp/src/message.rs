//! Moving a message across the wire boundary.
//!
//! The domain stores an exact encoded AMQP envelope beside the normalized
//! fields it needs for routing and expiry. On delivery, this adapter restores
//! that envelope and overlays only the fields the broker owns.

use amqp::{
    AnnotationKey, ApplicationProperties, Body, DeliveryAnnotations, Header, Message,
    MessageAnnotations, MessageId, Properties, Uuid, Value, decode_message,
};
use domain::{Delivery, MessageEnvelope, SessionId};
use serde_amqp::primitives::Timestamp as AmqpTimestamp;

use crate::{ProtocolError, parse_session_id};

/// What a client sent, reduced to what the broker keeps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingMessage {
    pub message_id: String,
    pub body: Vec<u8>,
    pub session_id: Option<SessionId>,
    pub time_to_live_millis: Option<u64>,
    pub envelope: MessageEnvelope,
}

/// Reads an incoming AMQP message into the parts a send command needs.
///
/// A message with no identifier of its own is accepted: Service Bus assigns one
/// rather than refusing the send, and the broker's own sequence number is what
/// actually identifies the message afterwards.
pub fn read_incoming(
    message: &Message,
    encoded_message: &[u8],
) -> Result<IncomingMessage, ProtocolError> {
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
        envelope: MessageEnvelope::new(encoded_message.to_vec()),
    })
}

/// The application property Service Bus clients read a dead-letter reason from.
pub const DEAD_LETTER_REASON_PROPERTY: &str = "DeadLetterReason";
/// The application property carrying the dead-letter description.
pub const DEAD_LETTER_DESCRIPTION_PROPERTY: &str = "DeadLetterErrorDescription";

const SEQUENCE_NUMBER_ANNOTATION: &str = "x-opt-sequence-number";
const ENQUEUE_SEQUENCE_NUMBER_ANNOTATION: &str = "x-opt-enqueue-sequence-number";
const ENQUEUED_TIME_ANNOTATION: &str = "x-opt-enqueued-time";
const LOCKED_UNTIL_ANNOTATION: &str = "x-opt-locked-until";
const LOCK_TOKEN_ANNOTATION: &str = "x-opt-lock-token";
const DEAD_LETTER_SOURCE_ANNOTATION: &str = "x-opt-deadletter-source";

/// Builds the message handed back to a receiving client.
pub fn write_delivery(delivery: &Delivery) -> Result<Message, ProtocolError> {
    write_delivery_from(delivery, None)
}

/// Builds a delivery while identifying the entity that originally
/// dead-lettered it, when it is being drained from a dead-letter queue.
pub(crate) fn write_delivery_from(
    delivery: &Delivery,
    dead_letter_source: Option<&str>,
) -> Result<Message, ProtocolError> {
    let mut message = match &delivery.envelope {
        Some(envelope) => {
            decode_message(envelope.as_bytes()).map_err(|error| ProtocolError::InvalidEnvelope {
                detail: error.to_string(),
            })?
        }
        None => {
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
            message
        }
    };

    overlay_delivery_header(&mut message, delivery);
    overlay_delivery_properties(&mut message, delivery);
    overlay_message_annotations(&mut message, delivery, dead_letter_source);
    overlay_lock_token(&mut message, delivery);

    // A message drained from a dead-letter queue says why it is there, in the
    // properties the Service Bus SDKs read. Custom application properties stay
    // alongside the broker-owned reason and description.
    match &delivery.dead_letter {
        Some(dead_letter) => {
            let properties = message
                .application_properties
                .get_or_insert_with(ApplicationProperties::default);
            properties.insert(
                DEAD_LETTER_REASON_PROPERTY,
                dead_letter.reason.as_str().to_owned(),
            );
            properties.insert(
                DEAD_LETTER_DESCRIPTION_PROPERTY,
                dead_letter.description.clone(),
            );
        }
        None => {
            if let Some(properties) = message.application_properties.as_mut() {
                properties.0.shift_remove(DEAD_LETTER_REASON_PROPERTY);
                properties.0.shift_remove(DEAD_LETTER_DESCRIPTION_PROPERTY);
            }
        }
    }
    Ok(message)
}

fn overlay_delivery_header(message: &mut Message, delivery: &Delivery) {
    let ttl = delivery.expires_at.map(|expires_at| {
        let millis = expires_at
            .as_millis()
            .saturating_sub(delivery.enqueued_at.as_millis());
        u32::try_from(millis).unwrap_or(u32::MAX)
    });
    let delivery_count = delivery.delivery_count.saturating_sub(1);

    // Service Bus always supplies a header on deliveries. In particular, the
    // official SDK expects the first delivery's explicit zero rather than an
    // omitted default when calculating its one-based DeliveryCount.
    let header = message.header.get_or_insert_with(Header::default);
    header.ttl = ttl;
    // AMQP counts prior unsuccessful delivery attempts; the domain and Service
    // Bus SDKs count the current attempt as well.
    header.delivery_count = delivery_count;
}

fn overlay_delivery_properties(message: &mut Message, delivery: &Delivery) {
    let absolute_expiry_time = delivery
        .expires_at
        .map(|expires_at| timestamp_millis(expires_at.as_millis()));
    match message.properties.as_mut() {
        Some(properties) => properties.absolute_expiry_time = absolute_expiry_time,
        None if absolute_expiry_time.is_some() => {
            message
                .properties
                .get_or_insert_with(Properties::default)
                .absolute_expiry_time = absolute_expiry_time;
        }
        None => {}
    }
}

fn overlay_message_annotations(
    message: &mut Message,
    delivery: &Delivery,
    dead_letter_source: Option<&str>,
) {
    let annotations = message
        .message_annotations
        .get_or_insert_with(MessageAnnotations::default);
    let sequence = i64::try_from(delivery.sequence.as_u64()).unwrap_or(i64::MAX);
    annotations.insert(SEQUENCE_NUMBER_ANNOTATION, Value::Long(sequence));
    annotations.insert(ENQUEUE_SEQUENCE_NUMBER_ANNOTATION, Value::Long(sequence));
    annotations.insert(
        ENQUEUED_TIME_ANNOTATION,
        Value::Timestamp(AmqpTimestamp::from_milliseconds(timestamp_millis(
            delivery.enqueued_at.as_millis(),
        ))),
    );
    match delivery.lock {
        Some(lock) => annotations.insert(
            LOCKED_UNTIL_ANNOTATION,
            Value::Timestamp(AmqpTimestamp::from_milliseconds(timestamp_millis(
                lock.locked_until.as_millis(),
            ))),
        ),
        None => {
            annotations
                .0
                .shift_remove(&AnnotationKey::from(LOCKED_UNTIL_ANNOTATION));
        }
    }
    match dead_letter_source {
        Some(source) => annotations.insert(DEAD_LETTER_SOURCE_ANNOTATION, source.to_owned()),
        None => {
            annotations
                .0
                .shift_remove(&AnnotationKey::from(DEAD_LETTER_SOURCE_ANNOTATION));
        }
    }
}

fn overlay_lock_token(message: &mut Message, delivery: &Delivery) {
    let key = AnnotationKey::from(LOCK_TOKEN_ANNOTATION);
    match delivery.lock {
        Some(lock) => {
            let annotations = message
                .delivery_annotations
                .get_or_insert_with(DeliveryAnnotations::default);
            let mut bytes = [0_u8; 16];
            bytes[8..].copy_from_slice(&lock.token.as_u64().to_be_bytes());
            annotations.insert(key, Value::Uuid(Uuid::from(bytes)));
        }
        None => {
            if let Some(annotations) = message.delivery_annotations.as_mut() {
                annotations.0.shift_remove(&key);
            }
        }
    }
}

fn timestamp_millis(millis: u64) -> i64 {
    i64::try_from(millis).unwrap_or(i64::MAX)
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

    fn read(message: &Message) -> Result<IncomingMessage, ProtocolError> {
        let encoded = amqp::encode_message(message).expect("the test message encodes");
        read_incoming(message, &encoded)
    }

    #[test]
    fn a_body_and_identifier_cross_in() -> Result<(), ProtocolError> {
        let properties = Properties {
            message_id: Some(String::from("order-1").into()),
            ..Properties::default()
        };
        let incoming = read(&sent(Some(properties), b"payload".to_vec()))?;

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
        let incoming = read(&sent(Some(properties), Vec::new()))?;

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
            read(&sent(Some(properties), Vec::new())),
            Err(ProtocolError::InvalidSessionId { .. })
        ));
    }

    #[test]
    fn a_message_without_properties_still_crosses() -> Result<(), ProtocolError> {
        // Service Bus assigns an identifier rather than refusing the send, and
        // the sequence number is what identifies the message afterwards.
        let incoming = read(&sent(None, b"payload".to_vec()))?;
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
            expires_at: None,
            delivery_count: 1,
            lock: Some(DeliveryLock {
                token: LockToken::new(1),
                locked_until: Timestamp::from_millis(100),
            }),
            session_id: Some(SessionId::new("cart-1").expect("a valid session id")),
            dead_letter: None,
            envelope: None,
        };

        // A round trip through the wire shape keeps what the broker recorded, so
        // a redelivery looks like the first attempt.
        let outgoing = write_delivery(&delivery)?;
        let incoming = read(&outgoing)?;
        assert_eq!(incoming.message_id, "order-1");
        assert_eq!(incoming.body, b"payload".to_vec());
        assert_eq!(
            incoming.session_id.as_ref().map(SessionId::as_str),
            Some("cart-1")
        );
        Ok(())
    }
}
