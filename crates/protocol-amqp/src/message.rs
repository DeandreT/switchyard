//! Moving a message across the wire boundary.
//!
//! The domain stores an exact encoded AMQP envelope beside the normalized
//! fields it needs for routing and expiry. On delivery, this adapter restores
//! that envelope and overlays only the fields the broker owns.

use amqp::{
    AnnotationKey, ApplicationProperties, Body, DeliveryAnnotations, Fields, Header, Message,
    MessageAnnotations, MessageId, Properties, Uuid, Value, decode_message, encode_message,
};
use domain::{Delivery, DeliveryOrigin, MessageEnvelope, MessageInput, SessionId, Timestamp};
use serde_amqp::primitives::Timestamp as AmqpTimestamp;

use crate::{ProtocolError, parse_session_id};

/// What a client sent, reduced to what the broker keeps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingMessage {
    pub message_id: String,
    pub body: Vec<u8>,
    pub session_id: Option<SessionId>,
    pub time_to_live_millis: Option<u64>,
    pub scheduled_enqueue_at: Option<Timestamp>,
    pub envelope: MessageEnvelope,
}

impl From<IncomingMessage> for MessageInput {
    fn from(message: IncomingMessage) -> Self {
        Self {
            message_id: message.message_id,
            body: message.body,
            session_id: message.session_id,
            time_to_live_millis: message.time_to_live_millis,
            scheduled_enqueue_at: message.scheduled_enqueue_at,
            envelope: Some(message.envelope),
        }
    }
}

/// The messages represented by one incoming AMQP delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncomingMessages {
    Single(IncomingMessage),
    Batch(Vec<IncomingMessage>),
}

/// Microsoft Service Bus extension format whose Data sections each contain a
/// complete encoded child AMQP message.
pub const SERVICE_BUS_BATCH_MESSAGE_FORMAT: u32 = 0x8001_3700;

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
        scheduled_enqueue_at: scheduled_enqueue_at(message)?,
        envelope: MessageEnvelope::new(encoded_message.to_vec()),
    })
}

/// Expands the message-format on one transfer into broker message inputs.
///
/// Ordinary AMQP messages use format zero. A Service Bus batch is an outer
/// message whose body consists exclusively of Data sections, with each Data
/// value holding one complete encoded AMQP child message. The child bytes are
/// retained verbatim as their lossless envelopes.
pub fn read_incoming_messages(
    message_format: u32,
    message: &Message,
    encoded_message: &[u8],
) -> Result<IncomingMessages, ProtocolError> {
    match message_format {
        0 => read_incoming(message, encoded_message).map(IncomingMessages::Single),
        SERVICE_BUS_BATCH_MESSAGE_FORMAT => read_batch(message).map(IncomingMessages::Batch),
        message_format => Err(ProtocolError::UnsupportedMessageFormat { message_format }),
    }
}

fn read_batch(message: &Message) -> Result<Vec<IncomingMessage>, ProtocolError> {
    let Body::Data(sections) = &message.body else {
        return Err(ProtocolError::InvalidBatch {
            detail: String::from("the Service Bus batch body must contain only AMQP Data sections"),
        });
    };
    if sections.is_empty() {
        return Err(ProtocolError::InvalidBatch {
            detail: String::from("the Service Bus batch contains no child messages"),
        });
    }

    sections
        .iter()
        .enumerate()
        .map(|(index, encoded)| {
            let child = decode_message(encoded).map_err(|error| ProtocolError::InvalidBatch {
                detail: format!("child message {index} is not a complete AMQP message: {error}"),
            })?;
            read_incoming(&child, encoded).map_err(|error| ProtocolError::InvalidBatch {
                detail: format!("child message {index} is invalid: {error}"),
            })
        })
        .collect()
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
const MESSAGE_STATE_ANNOTATION: &str = "x-opt-message-state";
const SCHEDULED_ENQUEUE_TIME_ANNOTATION: &str = "x-opt-scheduled-enqueue-time";

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
    write_delivery_view(delivery, dead_letter_source, DeliveryView::Receive)
}

/// Builds the embedded message returned by the Service Bus peek management
/// operation. A peek describes durable broker state, not a new delivery: it
/// reports the stored delivery count exactly and never exposes a message lock.
pub(crate) fn write_peeked_delivery_from(
    delivery: &Delivery,
    dead_letter_source: Option<&str>,
) -> Result<Message, ProtocolError> {
    write_delivery_view(delivery, dead_letter_source, DeliveryView::Peek)
}

#[derive(Clone, Copy)]
enum DeliveryView {
    Receive,
    Peek,
}

fn write_delivery_view(
    delivery: &Delivery,
    dead_letter_source: Option<&str>,
    view: DeliveryView,
) -> Result<Message, ProtocolError> {
    let mut message = stored_message(delivery)?;

    overlay_delivery_header(&mut message, delivery, view);
    overlay_delivery_properties(&mut message, delivery);
    overlay_message_annotations(&mut message, delivery, dead_letter_source, view);
    overlay_lock_token(&mut message, delivery, view);
    overlay_dead_letter_properties(&mut message, delivery);

    Ok(message)
}

fn overlay_dead_letter_properties(message: &mut Message, delivery: &Delivery) {
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
}

fn stored_message(delivery: &Delivery) -> Result<Message, ProtocolError> {
    Ok(match &delivery.envelope {
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
    })
}

/// Applies Service Bus disposition property changes to the durable envelope.
///
/// Microsoft carries application-property updates in the `message-annotations`
/// field of a Modified outcome and in a similarly shaped management map. They
/// are applied to the sender's original envelope, before broker-owned delivery
/// overlays are added, so sequence, lock, and enqueue annotations can never be
/// persisted accidentally.
pub(crate) fn replacement_envelope(
    delivery: &Delivery,
    properties: &Fields,
) -> Result<MessageEnvelope, ProtocolError> {
    let mut message = stored_message(delivery)?;
    let application_properties = message
        .application_properties
        .get_or_insert_with(ApplicationProperties::default);
    for (name, value) in properties {
        application_properties.insert(name.as_str(), value.clone());
    }
    encode_message(&message)
        .map(MessageEnvelope::new)
        .map_err(|error| ProtocolError::InvalidEnvelope {
            detail: error.to_string(),
        })
}

fn overlay_delivery_header(message: &mut Message, delivery: &Delivery, view: DeliveryView) {
    let ttl = delivery.expires_at.map(|expires_at| {
        let lifetime_start = match delivery.origin {
            DeliveryOrigin::Scheduled => delivery
                .scheduled_enqueue_at
                .unwrap_or(delivery.enqueued_at),
            DeliveryOrigin::Ready | DeliveryOrigin::Deferred => delivery.enqueued_at,
        };
        let millis = expires_at
            .as_millis()
            .saturating_sub(lifetime_start.as_millis());
        u32::try_from(millis).unwrap_or(u32::MAX)
    });
    let delivery_count = match view {
        DeliveryView::Receive => delivery.delivery_count.saturating_sub(1),
        DeliveryView::Peek => delivery.delivery_count,
    };

    // Service Bus always supplies a header. Receive transfers report prior
    // attempts, while the peek management response reports the stored count.
    let header = message.header.get_or_insert_with(Header::default);
    header.ttl = ttl;
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
    view: DeliveryView,
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
    match delivery.scheduled_enqueue_at {
        Some(scheduled_enqueue_at) => annotations.insert(
            SCHEDULED_ENQUEUE_TIME_ANNOTATION,
            Value::Timestamp(AmqpTimestamp::from_milliseconds(timestamp_millis(
                scheduled_enqueue_at.as_millis(),
            ))),
        ),
        None => {
            annotations
                .0
                .shift_remove(&AnnotationKey::from(SCHEDULED_ENQUEUE_TIME_ANNOTATION));
        }
    }
    match (view, delivery.lock) {
        (DeliveryView::Receive, Some(lock)) => annotations.insert(
            LOCKED_UNTIL_ANNOTATION,
            Value::Timestamp(AmqpTimestamp::from_milliseconds(timestamp_millis(
                lock.locked_until.as_millis(),
            ))),
        ),
        (DeliveryView::Receive, None) | (DeliveryView::Peek, _) => {
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
    match delivery.origin {
        DeliveryOrigin::Deferred => {
            // Service Bus numbers Active=0, Deferred=1, Scheduled=2.
            annotations.insert(MESSAGE_STATE_ANNOTATION, Value::Int(1));
        }
        DeliveryOrigin::Scheduled => {
            annotations.insert(MESSAGE_STATE_ANNOTATION, Value::Int(2));
        }
        DeliveryOrigin::Ready => {
            annotations
                .0
                .shift_remove(&AnnotationKey::from(MESSAGE_STATE_ANNOTATION));
        }
    }
}

fn scheduled_enqueue_at(message: &Message) -> Result<Option<Timestamp>, ProtocolError> {
    let Some(value) = message
        .message_annotations
        .as_ref()
        .and_then(|annotations| {
            annotations.get(&AnnotationKey::from(SCHEDULED_ENQUEUE_TIME_ANNOTATION))
        })
    else {
        return Ok(None);
    };
    let Value::Timestamp(value) = value else {
        return Err(ProtocolError::InvalidScheduledEnqueueTime {
            detail: String::from("x-opt-scheduled-enqueue-time must be an AMQP timestamp"),
        });
    };
    let millis = u64::try_from(value.milliseconds()).map_err(|_| {
        ProtocolError::InvalidScheduledEnqueueTime {
            detail: String::from("x-opt-scheduled-enqueue-time cannot precede the Unix epoch"),
        }
    })?;
    Ok(Some(Timestamp::from_millis(millis)))
}

fn overlay_lock_token(message: &mut Message, delivery: &Delivery, view: DeliveryView) {
    let key = AnnotationKey::from(LOCK_TOKEN_ANNOTATION);
    match (view, delivery.lock) {
        (DeliveryView::Receive, Some(lock)) => {
            let annotations = message
                .delivery_annotations
                .get_or_insert_with(DeliveryAnnotations::default);
            let mut bytes = [0_u8; 16];
            bytes[8..].copy_from_slice(&lock.token.as_u64().to_be_bytes());
            annotations.insert(key, Value::Uuid(Uuid::from(bytes)));
        }
        (DeliveryView::Receive, None) | (DeliveryView::Peek, _) => {
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
    fn a_scheduled_enqueue_timestamp_crosses_in_as_broker_state() -> Result<(), ProtocolError> {
        let mut message = sent(None, b"later".to_vec());
        let mut annotations = MessageAnnotations::default();
        annotations.insert(
            SCHEDULED_ENQUEUE_TIME_ANNOTATION,
            Value::Timestamp(AmqpTimestamp::from_milliseconds(12_345)),
        );
        message.message_annotations = Some(annotations);

        assert_eq!(
            read(&message)?.scheduled_enqueue_at,
            Some(Timestamp::from_millis(12_345))
        );
        Ok(())
    }

    #[test]
    fn a_malformed_scheduled_enqueue_annotation_is_not_ignored() {
        let mut message = sent(None, b"later".to_vec());
        let mut annotations = MessageAnnotations::default();
        annotations.insert(SCHEDULED_ENQUEUE_TIME_ANNOTATION, Value::Long(12_345));
        message.message_annotations = Some(annotations);

        assert!(matches!(
            read(&message),
            Err(ProtocolError::InvalidScheduledEnqueueTime { .. })
        ));
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
    fn a_service_bus_batch_expands_complete_child_envelopes() -> Result<(), ProtocolError> {
        let first = sent(
            Some(Properties {
                message_id: Some(String::from("order-1").into()),
                group_id: Some(String::from("cart-1")),
                ..Properties::default()
            }),
            b"first".to_vec(),
        );
        let second = sent(
            Some(Properties {
                message_id: Some(String::from("order-2").into()),
                ..Properties::default()
            }),
            b"second".to_vec(),
        );
        let first_encoded = amqp::encode_message(&first).expect("the first child encodes");
        let second_encoded = amqp::encode_message(&second).expect("the second child encodes");
        let outer = Message {
            body: Body::Data(vec![
                first_encoded.clone().into(),
                second_encoded.clone().into(),
            ]),
            ..Message::default()
        };

        let IncomingMessages::Batch(children) =
            read_incoming_messages(SERVICE_BUS_BATCH_MESSAGE_FORMAT, &outer, &[])?
        else {
            panic!("the batched format must produce a batch")
        };
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].message_id, "order-1");
        assert_eq!(children[0].body, b"first");
        assert_eq!(
            children[0].session_id.as_ref().map(SessionId::as_str),
            Some("cart-1")
        );
        assert_eq!(children[0].envelope.as_bytes(), first_encoded);
        assert_eq!(children[1].message_id, "order-2");
        assert_eq!(children[1].body, b"second");
        assert_eq!(children[1].envelope.as_bytes(), second_encoded);
        Ok(())
    }

    #[test]
    fn an_empty_or_non_data_service_bus_batch_is_rejected() {
        for message in [
            Message::default(),
            Message {
                body: Body::Value(Value::String(String::from("not a batch"))),
                ..Message::default()
            },
        ] {
            assert!(matches!(
                read_incoming_messages(SERVICE_BUS_BATCH_MESSAGE_FORMAT, &message, &[]),
                Err(ProtocolError::InvalidBatch { .. })
            ));
        }
    }

    #[test]
    fn one_malformed_child_rejects_the_whole_service_bus_batch() {
        let valid =
            amqp::encode_message(&sent(None, b"valid".to_vec())).expect("the valid child encodes");
        let outer = Message {
            body: Body::Data(vec![valid.into(), b"not an AMQP message".to_vec().into()]),
            ..Message::default()
        };

        assert!(matches!(
            read_incoming_messages(SERVICE_BUS_BATCH_MESSAGE_FORMAT, &outer, &[]),
            Err(ProtocolError::InvalidBatch { .. })
        ));
    }

    #[test]
    fn an_unknown_message_format_is_rejected_without_reinterpreting_the_body() {
        let message = sent(None, b"ordinary".to_vec());
        assert_eq!(
            read_incoming_messages(7, &message, &[]),
            Err(ProtocolError::UnsupportedMessageFormat { message_format: 7 })
        );
    }

    #[test]
    fn ordinary_multiple_data_sections_remain_one_message() -> Result<(), ProtocolError> {
        let message = Message {
            body: Body::Data(vec![b"one".to_vec().into(), b"two".to_vec().into()]),
            ..Message::default()
        };
        let encoded = amqp::encode_message(&message).expect("the ordinary message encodes");

        let IncomingMessages::Single(incoming) = read_incoming_messages(0, &message, &encoded)?
        else {
            panic!("format zero must never be inferred to be a batch")
        };
        assert_eq!(incoming.body, b"onetwo");
        assert_eq!(incoming.envelope.as_bytes(), encoded);
        Ok(())
    }

    #[test]
    fn a_delivery_carries_its_identifier_and_session_back_out() -> Result<(), ProtocolError> {
        let delivery = Delivery {
            sequence: SequenceNumber::new(7),
            message_id: String::from("order-1"),
            body: b"payload".to_vec(),
            enqueued_at: Timestamp::from_millis(10),
            scheduled_enqueue_at: None,
            expires_at: None,
            delivery_count: 1,
            lock: Some(DeliveryLock {
                token: LockToken::new(1),
                locked_until: Timestamp::from_millis(100),
                lock_duration_millis: 60_000,
            }),
            session_id: Some(SessionId::new("cart-1").expect("a valid session id")),
            dead_letter: None,
            envelope: None,
            origin: DeliveryOrigin::Ready,
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

    #[test]
    fn a_deferred_delivery_carries_the_service_bus_message_state() -> Result<(), ProtocolError> {
        let mut delivery = Delivery {
            sequence: SequenceNumber::new(7),
            message_id: String::from("deferred"),
            body: b"payload".to_vec(),
            enqueued_at: Timestamp::from_millis(10),
            scheduled_enqueue_at: None,
            expires_at: None,
            delivery_count: 1,
            lock: Some(DeliveryLock {
                token: LockToken::new(1),
                locked_until: Timestamp::from_millis(100),
                lock_duration_millis: 60_000,
            }),
            session_id: None,
            dead_letter: None,
            envelope: None,
            origin: DeliveryOrigin::Deferred,
        };

        let deferred = write_delivery(&delivery)?;
        let state_key = AnnotationKey::from(MESSAGE_STATE_ANNOTATION);
        assert_eq!(
            deferred
                .message_annotations
                .as_ref()
                .and_then(|annotations| annotations.get(&state_key)),
            Some(&Value::Int(1))
        );

        // State is broker-owned: an active delivery must clear a sender-forged
        // scheduled/deferred value rather than reflecting it.
        let mut forged = Message::data(b"payload".to_vec());
        let mut forged_annotations = MessageAnnotations::default();
        forged_annotations.insert(MESSAGE_STATE_ANNOTATION, Value::Long(2));
        forged.message_annotations = Some(forged_annotations);
        delivery.envelope = Some(MessageEnvelope::new(
            encode_message(&forged).expect("the forged envelope encodes"),
        ));
        delivery.origin = DeliveryOrigin::Ready;
        let active = write_delivery(&delivery)?;
        assert_eq!(
            active
                .message_annotations
                .as_ref()
                .and_then(|annotations| annotations.get(&state_key)),
            None
        );
        Ok(())
    }

    #[test]
    fn a_scheduled_peek_has_state_two_and_keeps_its_requested_time() -> Result<(), ProtocolError> {
        let delivery = Delivery {
            sequence: SequenceNumber::new(7),
            message_id: String::from("scheduled"),
            body: b"later".to_vec(),
            enqueued_at: Timestamp::from_millis(10),
            scheduled_enqueue_at: Some(Timestamp::from_millis(12_345)),
            expires_at: Some(Timestamp::from_millis(15_345)),
            delivery_count: 0,
            lock: None,
            session_id: None,
            dead_letter: None,
            envelope: None,
            origin: DeliveryOrigin::Scheduled,
        };

        let message = write_peeked_delivery_from(&delivery, None)?;
        let annotations = message.message_annotations.expect("broker annotations");
        assert_eq!(
            annotations.get(&AnnotationKey::from(MESSAGE_STATE_ANNOTATION)),
            Some(&Value::Int(2))
        );
        assert!(matches!(
            annotations.get(&AnnotationKey::from(SCHEDULED_ENQUEUE_TIME_ANNOTATION)),
            Some(Value::Timestamp(value)) if value.milliseconds() == 12_345
        ));
        assert_eq!(
            message.header.as_ref().and_then(|header| header.ttl),
            Some(3_000),
            "scheduled peek TTL begins at scheduled enqueue, not acceptance"
        );
        assert!(
            !annotations
                .0
                .contains_key(&AnnotationKey::from(LOCKED_UNTIL_ANNOTATION))
        );
        assert!(
            message
                .delivery_annotations
                .as_ref()
                .is_none_or(|annotations| {
                    !annotations
                        .0
                        .contains_key(&AnnotationKey::from(LOCK_TOKEN_ANNOTATION))
                })
        );
        Ok(())
    }
}
