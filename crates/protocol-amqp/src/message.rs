//! Moving a message across the wire boundary.
//!
//! The domain stores an exact encoded AMQP envelope beside the normalized
//! fields it needs for routing and expiry. On delivery, this adapter restores
//! that envelope and overlays only the fields the broker owns.

use amqp::{
    AnnotationKey, ApplicationProperties, Body, DeliveryAnnotations, Fields, Header, Message,
    MessageAnnotations, MessageId, Properties, Uuid, Value, decode_message, decode_stored_message,
    decode_stored_value, decode_value,
};
use domain::{
    CONTENT_LIMITS, CorrelationValue, Delivery, DeliveryOrigin, FilterProperties, MessageContent,
    MessageEnvelope, MessageInput, OverlayProperty, SessionId, Timestamp,
};
use serde_amqp::primitives::Timestamp as AmqpTimestamp;

use crate::{ProtocolError, parse_session_id};

/// What a client sent, reduced to the fields a legacy V1 send carried.
///
/// Ingress now sends original content and the state machine derives these
/// fields; this reading stays as the oracle that derivation is checked against.
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

/// The original content of each message one incoming AMQP delivery carries.
/// The state machine derives every normalized field from these bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncomingMessages {
    Single(MessageContent),
    Batch(Vec<MessageContent>),
}

/// Microsoft Service Bus extension format whose Data sections each contain a
/// complete encoded child AMQP message.
pub const SERVICE_BUS_BATCH_MESSAGE_FORMAT: u32 = 0x8001_3700;

/// Reads an incoming AMQP message into the parts a legacy V1 send carried.
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
        envelope: MessageEnvelope::new(encoded_message.to_vec())
            .with_filter_properties(filter_properties(message)?),
    })
}

/// Expands the message-format on one transfer into original message content.
///
/// Ordinary AMQP messages use format zero; the transfer was already decoded
/// with the received-message checks, and its encoded bytes are the content. A
/// Service Bus batch is an outer message whose body consists exclusively of
/// Data sections, with each Data value holding one complete encoded AMQP child
/// message. Each child passes the same received checks, then its bytes are
/// retained verbatim as its content.
pub fn read_incoming_messages(
    message_format: u32,
    message: &Message,
    encoded_message: &[u8],
) -> Result<IncomingMessages, ProtocolError> {
    match message_format {
        0 => Ok(IncomingMessages::Single(MessageContent::Amqp(
            encoded_message.to_vec(),
        ))),
        SERVICE_BUS_BATCH_MESSAGE_FORMAT => read_batch(message).map(IncomingMessages::Batch),
        message_format => Err(ProtocolError::UnsupportedMessageFormat { message_format }),
    }
}

fn read_batch(message: &Message) -> Result<Vec<MessageContent>, ProtocolError> {
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
            // Children arrive inside the sender's transfer: received checks.
            decode_message(encoded).map_err(|error| ProtocolError::InvalidBatch {
                detail: format!("child message {index} is not a complete AMQP message: {error}"),
            })?;
            Ok(MessageContent::Amqp(encoded.to_vec()))
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

/// Builds a delivery with the original dead-letter entity when a future
/// auto-forwarding path moves it out of a DLQ. Direct DLQ drains pass `None`.
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

    // The copy's own property values come first, so every broker-owned field
    // below overrides whatever the sender or a patch put in its place.
    overlay_copy_properties(&mut message, delivery)?;
    overlay_delivery_header(&mut message, delivery, view);
    overlay_delivery_properties(&mut message, delivery);
    overlay_message_annotations(&mut message, delivery, dead_letter_source, view);
    overlay_lock_token(&mut message, delivery, view);
    overlay_dead_letter_properties(&mut message, delivery);

    Ok(message)
}

/// Applies the copy-local overlay a property-modifying disposition left, in
/// stored order: a name already present keeps its position and takes the
/// overlay value, a new name is appended, and an explicit null renders as an
/// AMQP null rather than removing the property. The broker's filter
/// projection keys properties by name, so it does not depend on this order.
fn overlay_copy_properties(
    message: &mut Message,
    delivery: &Delivery,
) -> Result<(), ProtocolError> {
    let Some(envelope) = delivery.envelope.as_ref() else {
        return Ok(());
    };
    if envelope.overlay().is_empty() {
        return Ok(());
    }
    let properties = message
        .application_properties
        .get_or_insert_with(ApplicationProperties::default);
    for property in envelope.overlay() {
        let value = decode_stored_value(&property.value).map_err(|error| {
            ProtocolError::InvalidEnvelope {
                detail: format!(
                    "the stored value of property {:?} does not decode: {error}",
                    property.name
                ),
            }
        })?;
        properties.insert(property.name.as_str(), value);
    }
    Ok(())
}

/// Bytes a rendered copy is bounded by before any of it is decoded: the
/// stored original plus every overlay name and value. The broker charges a
/// patched copy by the same measure and caps it at the same ceiling, so a
/// copy over it is state the broker should never have accepted.
fn check_render_ceiling(envelope: &MessageEnvelope) -> Result<(), ProtocolError> {
    let rendered_bytes = envelope
        .overlay()
        .iter()
        .fold(envelope.len(), |total, property| {
            total
                .saturating_add(property.name.len())
                .saturating_add(property.value.len())
        });
    if rendered_bytes > CONTENT_LIMITS.max_bytes {
        return Err(ProtocolError::InvalidEnvelope {
            detail: format!(
                "the stored copy needs {rendered_bytes} bytes to render, over the {}-byte ceiling",
                CONTENT_LIMITS.max_bytes
            ),
        });
    }
    Ok(())
}

fn overlay_dead_letter_properties(message: &mut Message, delivery: &Delivery) {
    // A message drained from a dead-letter queue says why it is there, in the
    // properties the Service Bus SDKs read. Custom application properties stay
    // alongside the broker-owned reason and description, which come last: a
    // sender's or patch's value for either name never survives, nor keeps its
    // place.
    if let Some(properties) = message.application_properties.as_mut() {
        properties.0.shift_remove(DEAD_LETTER_REASON_PROPERTY);
        properties.0.shift_remove(DEAD_LETTER_DESCRIPTION_PROPERTY);
    }
    if let Some(dead_letter) = &delivery.dead_letter {
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
}

fn stored_message(delivery: &Delivery) -> Result<Message, ProtocolError> {
    Ok(match &delivery.envelope {
        // Stored envelopes may predate the received-message checks; receive,
        // peek and deferred retrieval must keep decoding what the broker
        // already accepted.
        Some(envelope) => {
            check_render_ceiling(envelope)?;
            decode_stored_message(envelope.as_bytes()).map_err(|error| {
                ProtocolError::InvalidEnvelope {
                    detail: error.to_string(),
                }
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

/// Reads Service Bus disposition property changes into a copy-local patch.
///
/// Microsoft carries application-property updates in the `message-annotations`
/// field of a Modified outcome, in the info map of a dead-letter rejection and
/// in the management `properties-to-modify` map. Each value is encoded on its
/// own and must pass the received-message checks, so the broker stores only
/// values a peer could have sent. The dead-letter reason and description are
/// broker-owned and never become patch entries; the broker merges the patch
/// over the stored copy, never over the broker-owned delivery fields.
pub(crate) fn property_patch(properties: &Fields) -> Result<Vec<OverlayProperty>, ProtocolError> {
    properties
        .iter()
        .filter(|(name, _)| !is_reserved_dead_letter_property(name.as_str()))
        .map(|(name, value)| {
            let invalid = |error: &dyn std::fmt::Display| ProtocolError::InvalidEnvelope {
                detail: format!(
                    "property {:?} cannot be modified to this value: {error}",
                    name.as_str()
                ),
            };
            let encoded = serde_amqp::to_vec(value).map_err(|error| invalid(&error))?;
            decode_value(&encoded).map_err(|error| invalid(&error))?;
            Ok(OverlayProperty {
                name: name.as_str().to_owned(),
                value: encoded,
            })
        })
        .collect()
}

pub(crate) fn is_reserved_dead_letter_property(name: &str) -> bool {
    name == DEAD_LETTER_REASON_PROPERTY || name == DEAD_LETTER_DESCRIPTION_PROPERTY
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

/// Projects the AMQP properties correlation rules may inspect into replicated,
/// protocol-neutral state. Scalar encodings are canonicalized after decode so
/// the same logical AMQP type and value compare byte-for-byte whether they came
/// from a rule request or a publication.
fn filter_properties(message: &Message) -> Result<FilterProperties, ProtocolError> {
    let properties = message.properties.as_ref();
    let application_properties = message
        .application_properties
        .as_ref()
        .map(|properties| {
            properties
                .0
                .iter()
                .map(|(name, value)| Ok((name.clone(), correlation_value(value)?)))
                .collect::<Result<_, ProtocolError>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(FilterProperties {
        correlation_id: properties
            .and_then(|properties| properties.correlation_id.as_ref())
            .map(message_id_text),
        message_id: properties
            .and_then(|properties| properties.message_id.as_ref())
            .map(message_id_text),
        to: properties.and_then(|properties| properties.to.clone()),
        reply_to: properties.and_then(|properties| properties.reply_to.clone()),
        subject: properties.and_then(|properties| properties.subject.clone()),
        session_id: properties.and_then(|properties| properties.group_id.clone()),
        reply_to_session_id: properties.and_then(|properties| properties.reply_to_group_id.clone()),
        content_type: properties
            .and_then(|properties| properties.content_type.as_ref())
            .map(|content_type| content_type.as_str().to_owned()),
        application_properties,
    })
}

pub(crate) fn correlation_value(value: &Value) -> Result<CorrelationValue, ProtocolError> {
    let bytes = serde_amqp::to_vec(value).map_err(|error| ProtocolError::InvalidEnvelope {
        detail: format!("a filterable application property could not be encoded: {error}"),
    })?;
    CorrelationValue::new(bytes).map_err(|error| ProtocolError::InvalidEnvelope {
        detail: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use amqp::{Symbol, encode_message};
    use domain::{
        DeadLetterInfo, DeadLetterReason, DeliveryLock, LockToken, SequenceNumber, Timestamp,
    };

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
    fn every_correlation_field_crosses_into_the_replicated_filter_projection()
    -> Result<(), ProtocolError> {
        let message = Message::builder()
            .properties(Properties {
                message_id: Some(String::from("message-7").into()),
                correlation_id: Some(String::from("correlation-7").into()),
                to: Some(String::from("logical-destination")),
                reply_to: Some(String::from("logical-reply")),
                subject: Some(String::from("orders.created")),
                group_id: Some(String::from("session-7")),
                reply_to_group_id: Some(String::from("reply-session-7")),
                content_type: Some(Symbol::from("application/json")),
                ..Properties::default()
            })
            .application_properties(
                ApplicationProperties::builder()
                    .insert("priority", 7_i32)
                    .insert("region", "West")
                    .build(),
            )
            .build();

        let incoming = read(&message)?;
        let projected = incoming.envelope.filter_properties();
        assert_eq!(projected.message_id.as_deref(), Some("message-7"));
        assert_eq!(projected.correlation_id.as_deref(), Some("correlation-7"));
        assert_eq!(projected.to.as_deref(), Some("logical-destination"));
        assert_eq!(projected.reply_to.as_deref(), Some("logical-reply"));
        assert_eq!(projected.subject.as_deref(), Some("orders.created"));
        assert_eq!(projected.session_id.as_deref(), Some("session-7"));
        assert_eq!(
            projected.reply_to_session_id.as_deref(),
            Some("reply-session-7")
        );
        assert_eq!(projected.content_type.as_deref(), Some("application/json"));
        assert_eq!(
            projected.application_properties.get("priority"),
            Some(&correlation_value(&Value::Int(7))?)
        );
        assert_eq!(
            projected.application_properties.get("region"),
            Some(&correlation_value(&Value::String(String::from("West")))?)
        );
        assert_ne!(
            correlation_value(&Value::Int(7))?,
            correlation_value(&Value::Long(7))?,
            "correlation equality preserves the AMQP scalar type"
        );
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
        assert_eq!(
            children,
            vec![
                MessageContent::Amqp(first_encoded),
                MessageContent::Amqp(second_encoded)
            ]
        );
        let derived = MessageContent::derive_batch(&children).expect("the children derive");
        assert_eq!(derived[0].message_id, "order-1");
        assert_eq!(
            derived[0].session_id.as_ref().map(SessionId::as_str),
            Some("cart-1")
        );
        assert_eq!(derived[1].message_id, "order-2");
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
    fn a_batch_child_with_an_odd_count_map_is_invalid() {
        let mut odd =
            amqp::encode_message(&sent(None, b"odd".to_vec())).expect("the child encodes");
        // A footer map8 counting one element: a key and no value.
        odd.extend_from_slice(&[0x00, 0x53, 0x78, 0xc1, 0x03, 0x01, 0xa3, 0x00]);
        let outer = Message {
            body: Body::Data(vec![odd.into()]),
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

        assert_eq!(
            read_incoming_messages(0, &message, &encoded)?,
            IncomingMessages::Single(MessageContent::Amqp(encoded)),
            "format zero must never be inferred to be a batch"
        );
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
    fn a_padded_stored_envelope_still_delivers_and_peeks() -> Result<(), ProtocolError> {
        // The properties list declares one padding byte past its message id,
        // which the earlier length-only measurement accepted and stored.
        let mut padded = vec![0x00, 0x53, 0x73, 0xc0, 0x05, 0x01, 0xa1, 0x01, b'x', 0x40];
        padded.extend(
            amqp::encode_message(&Message::data(b"payload".to_vec())).expect("the body encodes"),
        );
        let delivery = Delivery {
            sequence: SequenceNumber::new(7),
            message_id: String::from("x"),
            body: b"payload".to_vec(),
            enqueued_at: Timestamp::from_millis(10),
            scheduled_enqueue_at: None,
            expires_at: None,
            delivery_count: 1,
            lock: None,
            session_id: None,
            dead_letter: None,
            envelope: Some(MessageEnvelope::new(padded.clone())),
            origin: DeliveryOrigin::Ready,
        };

        for message in [
            write_delivery(&delivery)?,
            write_peeked_delivery_from(&delivery, None)?,
        ] {
            assert_eq!(
                message
                    .properties
                    .and_then(|properties| properties.message_id),
                Some(MessageId::String(String::from("x")))
            );
            assert_eq!(message.body, Body::Data(vec![b"payload".to_vec().into()]));
        }

        // The same bytes arriving from a peer are refused.
        assert!(decode_message(&padded).is_err());
        let batch = Message {
            body: Body::Data(vec![padded.into()]),
            ..Message::default()
        };
        assert!(matches!(
            read_incoming_messages(SERVICE_BUS_BATCH_MESSAGE_FORMAT, &batch, &[]),
            Err(ProtocolError::InvalidBatch { .. })
        ));
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

    fn overlay_entry(name: &str, value: Value) -> OverlayProperty {
        OverlayProperty {
            name: name.to_owned(),
            value: serde_amqp::to_vec(&value).expect("the overlay value encodes"),
        }
    }

    fn patched_delivery(original: &Message, overlay: Vec<OverlayProperty>) -> Delivery {
        Delivery {
            sequence: SequenceNumber::new(7),
            message_id: String::new(),
            body: Vec::new(),
            enqueued_at: Timestamp::from_millis(10),
            scheduled_enqueue_at: None,
            expires_at: None,
            delivery_count: 2,
            lock: Some(DeliveryLock {
                token: LockToken::new(3),
                locked_until: Timestamp::from_millis(100),
                lock_duration_millis: 60_000,
            }),
            session_id: None,
            dead_letter: None,
            envelope: Some(
                MessageEnvelope::new(encode_message(original).expect("the original encodes"))
                    .with_overlay(overlay),
            ),
            origin: DeliveryOrigin::Ready,
        }
    }

    fn rendered_properties(message: &Message) -> Vec<(String, Value)> {
        message
            .application_properties
            .as_ref()
            .map(|properties| {
                properties
                    .0
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn views(delivery: &Delivery) -> Result<[Message; 2], ProtocolError> {
        Ok([
            write_delivery(delivery)?,
            write_peeked_delivery_from(delivery, None)?,
        ])
    }

    #[test]
    fn an_overlay_replaces_in_place_appends_new_names_and_keeps_null() -> Result<(), ProtocolError>
    {
        let original = Message::builder()
            .application_properties(
                ApplicationProperties::builder()
                    .insert("first", "original")
                    .insert("second", 2_i32)
                    .insert("third", "untouched")
                    .build(),
            )
            .body(Body::Data(vec![b"payload".to_vec().into()]))
            .build();
        let delivery = patched_delivery(
            &original,
            vec![
                overlay_entry("second", Value::String(String::from("patched"))),
                overlay_entry("added", Value::Long(9)),
                overlay_entry("first", Value::Null),
            ],
        );

        for message in views(&delivery)? {
            assert_eq!(
                rendered_properties(&message),
                vec![
                    (String::from("first"), Value::Null),
                    (
                        String::from("second"),
                        Value::String(String::from("patched"))
                    ),
                    (
                        String::from("third"),
                        Value::String(String::from("untouched"))
                    ),
                    (String::from("added"), Value::Long(9)),
                ]
            );
            assert_eq!(message.body, Body::Data(vec![b"payload".to_vec().into()]));
        }

        // A message sent without application properties gains them in order.
        let bare = patched_delivery(
            &Message::data(b"bare".to_vec()),
            vec![
                overlay_entry("b", Value::Int(1)),
                overlay_entry("a", Value::Int(2)),
            ],
        );
        for message in views(&bare)? {
            assert_eq!(
                rendered_properties(&message),
                vec![
                    (String::from("b"), Value::Int(1)),
                    (String::from("a"), Value::Int(2)),
                ]
            );
        }
        Ok(())
    }

    #[test]
    fn broker_dead_letter_details_win_over_content_and_overlay() -> Result<(), ProtocolError> {
        let original = Message::builder()
            .application_properties(
                ApplicationProperties::builder()
                    .insert(DEAD_LETTER_REASON_PROPERTY, "forged in content")
                    .insert("kept", 1_i32)
                    .build(),
            )
            .body(Body::Data(vec![b"payload".to_vec().into()]))
            .build();
        let overlay = vec![
            overlay_entry(
                DEAD_LETTER_DESCRIPTION_PROPERTY,
                Value::String(String::from("forged in overlay")),
            ),
            overlay_entry("reviewed", Value::Bool(true)),
        ];
        let mut delivery = patched_delivery(&original, overlay);

        // Active: neither forged value reaches the receiver.
        for message in views(&delivery)? {
            let properties = rendered_properties(&message);
            assert_eq!(
                properties,
                vec![
                    (String::from("kept"), Value::Int(1)),
                    (String::from("reviewed"), Value::Bool(true)),
                ]
            );
        }

        // Dead-lettered: the broker's reason and description replace both.
        delivery.dead_letter = Some(DeadLetterInfo {
            reason: DeadLetterReason::Application(String::from("InvalidOrder")),
            description: String::from("the order has no customer"),
            dead_lettered_at: Timestamp::from_millis(20),
        });
        for message in views(&delivery)? {
            assert_eq!(
                rendered_properties(&message),
                vec![
                    (String::from("kept"), Value::Int(1)),
                    (String::from("reviewed"), Value::Bool(true)),
                    (
                        String::from(DEAD_LETTER_REASON_PROPERTY),
                        Value::String(String::from("InvalidOrder"))
                    ),
                    (
                        String::from(DEAD_LETTER_DESCRIPTION_PROPERTY),
                        Value::String(String::from("the order has no customer"))
                    ),
                ],
                "broker details replace forged values and come last"
            );
        }
        Ok(())
    }

    #[test]
    fn forged_broker_annotations_are_overridden_or_stripped() -> Result<(), ProtocolError> {
        let mut message_annotations = MessageAnnotations::default();
        message_annotations.insert(SEQUENCE_NUMBER_ANNOTATION, Value::Long(999));
        message_annotations.insert(ENQUEUE_SEQUENCE_NUMBER_ANNOTATION, Value::Long(999));
        message_annotations.insert(
            ENQUEUED_TIME_ANNOTATION,
            Value::Timestamp(AmqpTimestamp::from_milliseconds(999)),
        );
        message_annotations.insert(
            LOCKED_UNTIL_ANNOTATION,
            Value::Timestamp(AmqpTimestamp::from_milliseconds(999)),
        );
        message_annotations.insert(DEAD_LETTER_SOURCE_ANNOTATION, "forged-source");
        message_annotations.insert(MESSAGE_STATE_ANNOTATION, Value::Int(2));
        message_annotations.insert(
            SCHEDULED_ENQUEUE_TIME_ANNOTATION,
            Value::Timestamp(AmqpTimestamp::from_milliseconds(999)),
        );
        message_annotations.insert("x-custom", "kept");
        let mut delivery_annotations = DeliveryAnnotations::default();
        delivery_annotations.insert(LOCK_TOKEN_ANNOTATION, Value::Uuid(Uuid::from([7_u8; 16])));
        delivery_annotations.insert("x-hop", "sender only");
        let original = Message {
            header: Some(Header {
                delivery_count: 50,
                ..Header::default()
            }),
            delivery_annotations: Some(delivery_annotations),
            message_annotations: Some(message_annotations),
            ..Message::data(b"payload".to_vec())
        };
        let delivery = patched_delivery(&original, vec![overlay_entry("p", Value::Int(1))]);

        let [received, peeked] = views(&delivery)?;
        for (message, locked) in [(&received, true), (&peeked, false)] {
            let annotations = message
                .message_annotations
                .as_ref()
                .expect("broker annotations are present");
            let get = |name: &str| annotations.get(&AnnotationKey::from(name));
            assert_eq!(get(SEQUENCE_NUMBER_ANNOTATION), Some(&Value::Long(7)));
            assert_eq!(
                get(ENQUEUE_SEQUENCE_NUMBER_ANNOTATION),
                Some(&Value::Long(7))
            );
            assert!(matches!(
                get(ENQUEUED_TIME_ANNOTATION),
                Some(Value::Timestamp(value)) if value.milliseconds() == 10
            ));
            assert_eq!(get(DEAD_LETTER_SOURCE_ANNOTATION), None);
            assert_eq!(get(MESSAGE_STATE_ANNOTATION), None);
            assert_eq!(get(SCHEDULED_ENQUEUE_TIME_ANNOTATION), None);
            assert_eq!(get("x-custom"), Some(&Value::String(String::from("kept"))));
            if locked {
                assert!(matches!(
                    get(LOCKED_UNTIL_ANNOTATION),
                    Some(Value::Timestamp(value)) if value.milliseconds() == 100
                ));
                let delivery_annotations = message
                    .delivery_annotations
                    .as_ref()
                    .expect("the broker lock token is present");
                let mut token = [0_u8; 16];
                token[8..].copy_from_slice(&3_u64.to_be_bytes());
                assert_eq!(
                    delivery_annotations.get(&AnnotationKey::from(LOCK_TOKEN_ANNOTATION)),
                    Some(&Value::Uuid(Uuid::from(token)))
                );
            } else {
                assert_eq!(get(LOCKED_UNTIL_ANNOTATION), None);
                assert_eq!(
                    message
                        .delivery_annotations
                        .as_ref()
                        .and_then(|annotations| {
                            annotations.get(&AnnotationKey::from(LOCK_TOKEN_ANNOTATION))
                        }),
                    None,
                    "a peek never carries a lock token, forged or not"
                );
            }
            assert_eq!(
                message
                    .delivery_annotations
                    .as_ref()
                    .and_then(|annotations| annotations.get(&AnnotationKey::from("x-hop"))),
                Some(&Value::String(String::from("sender only"))),
                "custom delivery annotations keep round-tripping"
            );
        }
        assert_eq!(
            received.header.as_ref().map(|header| header.delivery_count),
            Some(1)
        );
        assert_eq!(
            peeked.header.as_ref().map(|header| header.delivery_count),
            Some(2)
        );
        Ok(())
    }

    #[test]
    fn an_overlay_value_that_does_not_decode_is_an_invalid_envelope() {
        for value in [vec![], vec![0x40, 0x40], vec![0xc1, 0x03, 0x01, 0x54, 0x01]] {
            let delivery = patched_delivery(
                &Message::data(b"payload".to_vec()),
                vec![OverlayProperty {
                    name: String::from("broken"),
                    value,
                }],
            );
            assert!(matches!(
                write_delivery(&delivery),
                Err(ProtocolError::InvalidEnvelope { .. })
            ));
            assert!(matches!(
                write_peeked_delivery_from(&delivery, None),
                Err(ProtocolError::InvalidEnvelope { .. })
            ));
        }
    }

    /// A copy whose original plus one overlay name and binary value charge
    /// exactly `charged` bytes, the measure the broker caps patches by.
    fn charged_copy(charged: usize) -> (Delivery, usize) {
        let original = Message::data(b"payload".to_vec());
        let encoded_len = encode_message(&original)
            .expect("the original encodes")
            .len();
        // One-byte name plus a five-byte binary header.
        let payload = charged - encoded_len - 1 - 5;
        let mut value = vec![0xb0];
        value.extend_from_slice(&u32::try_from(payload).expect("fits").to_be_bytes());
        value.resize(5 + payload, 0);
        let delivery = patched_delivery(
            &original,
            vec![OverlayProperty {
                name: String::from("x"),
                value,
            }],
        );
        (delivery, payload)
    }

    #[test]
    fn a_copy_at_the_render_ceiling_renders_and_one_byte_over_refuses() -> Result<(), ProtocolError>
    {
        // The broker caps a patched copy at exactly this ceiling.
        let (at_cap, payload) = charged_copy(CONTENT_LIMITS.max_bytes);
        for message in views(&at_cap)? {
            assert!(matches!(
                message
                    .application_properties
                    .as_ref()
                    .and_then(|properties| properties.get("x")),
                Some(Value::Binary(value)) if value.len() == payload
            ));
        }

        let (over, _) = charged_copy(CONTENT_LIMITS.max_bytes + 1);
        assert!(matches!(
            write_delivery(&over),
            Err(ProtocolError::InvalidEnvelope { detail }) if detail.contains("ceiling")
        ));
        assert!(matches!(
            write_peeked_delivery_from(&over, None),
            Err(ProtocolError::InvalidEnvelope { detail }) if detail.contains("ceiling")
        ));
        Ok(())
    }

    #[test]
    fn every_patch_the_protocol_produces_renders_back() -> Result<(), ProtocolError> {
        let mut map = amqp::OrderedMap::new();
        map.insert(Value::String(String::from("k")), Value::Int(1));
        let values = [
            Value::Null,
            Value::Bool(false),
            Value::Ubyte(1),
            Value::Ushort(2),
            Value::Uint(3),
            Value::Ulong(4),
            Value::Byte(-1),
            Value::Short(-2),
            Value::Int(-3),
            Value::Long(-4),
            Value::Float(1.5.into()),
            Value::Double(2.5.into()),
            Value::Char('x'),
            Value::Timestamp(AmqpTimestamp::from_milliseconds(1_234)),
            Value::Uuid(Uuid::from([9_u8; 16])),
            Value::Binary(vec![1_u8, 2, 3].into()),
            Value::String(String::from("text")),
            Value::Symbol(Symbol::from("symbol")),
            Value::List(vec![Value::Int(1), Value::String(String::from("two"))]),
            Value::Map(map),
            Value::Array(amqp::Array::from(vec![Value::Int(1), Value::Int(2)])),
        ];
        let mut fields = Fields::new();
        for (index, value) in values.iter().enumerate() {
            fields.insert(Symbol::from(format!("p{index}")), value.clone());
        }
        let patch = property_patch(&fields)?;
        assert_eq!(patch.len(), values.len());
        let delivery = patched_delivery(&Message::data(b"payload".to_vec()), patch);
        for message in views(&delivery)? {
            let rendered = rendered_properties(&message);
            assert_eq!(rendered.len(), values.len());
            for ((name, rendered), (index, value)) in rendered.iter().zip(values.iter().enumerate())
            {
                assert_eq!(name, &format!("p{index}"));
                assert_eq!(rendered, value, "{name}");
            }
        }
        Ok(())
    }

    #[test]
    fn a_patch_never_carries_broker_owned_dead_letter_names() -> Result<(), ProtocolError> {
        let mut fields = Fields::new();
        fields.insert(Symbol::from(DEAD_LETTER_REASON_PROPERTY), Value::Int(1));
        fields.insert(Symbol::from("kept"), Value::Int(2));
        fields.insert(
            Symbol::from(DEAD_LETTER_DESCRIPTION_PROPERTY),
            Value::Int(3),
        );
        assert_eq!(
            property_patch(&fields)?,
            vec![overlay_entry("kept", Value::Int(2))]
        );
        Ok(())
    }
}
