//! Moving a message across the wire boundary.
//!
//! Message content is retained in protocol-neutral types. Delivery state belongs
//! to the broker, not the producer, and is overlaid when the message leaves.

use std::collections::BTreeMap;

use amqp::{
    AnnotationKey as AmqpAnnotationKey, Annotations, ApplicationProperties, Body, Header, Message,
    MessageId, Properties,
};
use domain::{
    AnnotationKey, Delivery, MessageBody, MessageDescriptor, MessageEnvelope, MessageHeader,
    MessageIdentifier, MessageProperties, MessageStatus, MessageValue, SessionId, Timestamp,
};
use serde_amqp::{
    Value,
    described::Described,
    descriptor::Descriptor,
    primitives::{Array, Symbol, Timestamp as AmqpTimestamp},
};

use crate::{ProtocolError, parse_session_id};

/// What a client sent, including content independent of delivery state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingMessage {
    pub message_id: String,
    pub body: Vec<u8>,
    pub session_id: Option<SessionId>,
    pub time_to_live_millis: Option<u64>,
    pub scheduled_enqueue_time: Option<Timestamp>,
    pub envelope: MessageEnvelope,
}

/// Reads an incoming AMQP message into the parts a send command needs.
///
/// A message with no identifier of its own is accepted. Its broker sequence
/// identifies the stored record, and duplicate detection treats anonymous
/// submissions independently.
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
        time_to_live_millis: time_to_live_millis(message),
        scheduled_enqueue_time: scheduled_enqueue_time(message)?,
        envelope: read_envelope(message),
    })
}

fn time_to_live_millis(message: &Message) -> Option<u64> {
    // SDKs pair timestamps to carry durations beyond the header's uint limit.
    message
        .properties
        .as_ref()
        .and_then(|properties| {
            let created = properties.creation_time?;
            let expires = properties.absolute_expiry_time?;
            u64::try_from(i128::from(expires) - i128::from(created)).ok()
        })
        .or_else(|| {
            message
                .header
                .as_ref()
                .and_then(|header| header.ttl)
                .map(u64::from)
        })
}

fn scheduled_enqueue_time(message: &Message) -> Result<Option<Timestamp>, ProtocolError> {
    let Some(value) = message
        .message_annotations
        .as_ref()
        .and_then(|annotations| annotations.get(Symbol::from(SCHEDULED_ENQUEUE_TIME_ANNOTATION)))
    else {
        return Ok(None);
    };
    match value {
        Value::Timestamp(value) => u64::try_from(value.milliseconds())
            .map(Timestamp::from_millis)
            .map(Some)
            .map_err(|_| ProtocolError::InvalidScheduledEnqueueTime),
        _ => Err(ProtocolError::InvalidScheduledEnqueueTime),
    }
}

/// The application property Service Bus clients read a dead-letter reason from.
pub const DEAD_LETTER_REASON_PROPERTY: &str = "DeadLetterReason";
/// The application property carrying the dead-letter description.
pub const DEAD_LETTER_DESCRIPTION_PROPERTY: &str = "DeadLetterErrorDescription";
const SEQUENCE_NUMBER_ANNOTATION: &str = "x-opt-sequence-number";
const ENQUEUED_TIME_ANNOTATION: &str = "x-opt-enqueued-time";
const LOCKED_UNTIL_ANNOTATION: &str = "x-opt-locked-until";
pub const SCHEDULED_ENQUEUE_TIME_ANNOTATION: &str = "x-opt-scheduled-enqueue-time";
pub const MESSAGE_STATE_ANNOTATION: &str = "x-opt-message-state";

/// Builds the message handed back to a receiving client.
pub fn write_delivery(delivery: &Delivery) -> Message {
    let mut message = match &delivery.envelope {
        Some(envelope) => write_envelope(envelope),
        None => {
            let mut message = Message::data(delivery.body.clone());
            message.properties = Some(Properties {
                message_id: Some(delivery.message_id.clone().into()),
                ..Properties::default()
            });
            message
        }
    };
    let header = message.header.get_or_insert_with(Header::default);
    header.delivery_count = delivery.delivery_count;
    header.first_acquirer = false;
    let properties = message.properties.get_or_insert_with(Properties::default);
    properties.group_id = delivery
        .session_id
        .as_ref()
        .map(|session_id| session_id.as_str().to_owned());
    apply_expiration(header, properties, delivery);
    message.message_annotations = Some(message_annotations(
        delivery,
        message.message_annotations.take().unwrap_or_default(),
    ));

    let properties = message
        .application_properties
        .get_or_insert_with(ApplicationProperties::default);
    properties.0.shift_remove(DEAD_LETTER_REASON_PROPERTY);
    properties.0.shift_remove(DEAD_LETTER_DESCRIPTION_PROPERTY);
    if let Some(dead_letter) = &delivery.dead_letter {
        properties.insert(
            DEAD_LETTER_REASON_PROPERTY,
            dead_letter.reason.as_str().to_owned(),
        );
        properties.insert(
            DEAD_LETTER_DESCRIPTION_PROPERTY,
            dead_letter.description.clone(),
        );
    }
    message
}

fn apply_expiration(header: &mut Header, properties: &mut Properties, delivery: &Delivery) {
    let Some(lifetime) = delivery.time_to_live_millis else {
        header.ttl = None;
        properties.absolute_expiry_time = None;
        return;
    };
    header.ttl = Some(u32::try_from(lifetime).unwrap_or(u32::MAX));
    if let Some(expires_at) = delivery.expires_at {
        // Clients reconstruct TTL from this pair and expose absolute expiry as
        // ExpiresAt, so both timestamps must follow the broker's lifetime.
        properties.creation_time = Some(timestamp_millis(delivery.enqueued_at));
        properties.absolute_expiry_time = Some(timestamp_millis(expires_at));
        return;
    }

    // A pending schedule has no expiry deadline yet. Retain its producer pair
    // if it carries the same duration; otherwise only long TTLs need a pair.
    let pair_lifetime = properties
        .creation_time
        .zip(properties.absolute_expiry_time)
        .and_then(|(created, expires)| {
            u64::try_from(i128::from(expires) - i128::from(created)).ok()
        });
    if pair_lifetime == Some(lifetime) {
        return;
    }
    properties.absolute_expiry_time = None;
    if lifetime > u64::from(u32::MAX) {
        let created = properties.creation_time.unwrap_or_else(|| {
            timestamp_millis(
                delivery
                    .scheduled_enqueue_time
                    .unwrap_or(delivery.enqueued_at),
            )
        });
        properties.creation_time = Some(created);
        properties.absolute_expiry_time =
            Some(i64::try_from(i128::from(created) + i128::from(lifetime)).unwrap_or(i64::MAX));
    }
}

fn message_annotations(delivery: &Delivery, annotations: Annotations) -> Annotations {
    let mut annotations = Annotations(
        annotations
            .0
            .into_iter()
            .filter(|(key, _)| {
                !matches!(
                    key,
                    AmqpAnnotationKey::Symbol(symbol)
                        if matches!(symbol.as_str(),
                            SEQUENCE_NUMBER_ANNOTATION | ENQUEUED_TIME_ANNOTATION
                            | LOCKED_UNTIL_ANNOTATION | MESSAGE_STATE_ANNOTATION
                            | SCHEDULED_ENQUEUE_TIME_ANNOTATION)
                )
            })
            .collect(),
    );
    annotations.insert(
        Symbol::from(SEQUENCE_NUMBER_ANNOTATION),
        Value::Long(i64::try_from(delivery.sequence.as_u64()).unwrap_or(i64::MAX)),
    );
    annotations.insert(
        Symbol::from(ENQUEUED_TIME_ANNOTATION),
        Value::Timestamp(AmqpTimestamp::from_milliseconds(timestamp_millis(
            delivery.enqueued_at,
        ))),
    );
    annotations.insert(
        Symbol::from(MESSAGE_STATE_ANNOTATION),
        Value::Int(match delivery.status {
            MessageStatus::Active => 0,
            MessageStatus::Deferred => 1,
            MessageStatus::Scheduled => 2,
        }),
    );
    if let Some(scheduled) = delivery.scheduled_enqueue_time {
        annotations.insert(
            Symbol::from(SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(AmqpTimestamp::from_milliseconds(timestamp_millis(
                scheduled,
            ))),
        );
    }
    if let Some(lock) = &delivery.lock {
        annotations.insert(
            Symbol::from(LOCKED_UNTIL_ANNOTATION),
            Value::Timestamp(AmqpTimestamp::from_milliseconds(timestamp_millis(
                lock.locked_until,
            ))),
        );
    }
    annotations
}

fn timestamp_millis(timestamp: Timestamp) -> i64 {
    i64::try_from(timestamp.as_millis()).unwrap_or(i64::MAX)
}

fn read_envelope(message: &Message) -> MessageEnvelope {
    MessageEnvelope {
        header: message.header.as_ref().map(|header| MessageHeader {
            durable: header.durable,
            priority: header.priority,
            first_acquirer: header.first_acquirer,
        }),
        properties: message
            .properties
            .as_ref()
            .map(read_properties)
            .unwrap_or_default(),
        application_properties: message
            .application_properties
            .as_ref()
            .map(|properties| {
                properties
                    .0
                    .iter()
                    .map(|(key, value)| (key.clone(), read_value(value)))
                    .collect()
            })
            .unwrap_or_default(),
        message_annotations: read_annotations(message.message_annotations.as_ref()),
        footer: read_annotations(message.footer.as_ref()),
        body: match &message.body {
            Body::Empty => MessageBody::Empty,
            Body::Data(sections) => {
                MessageBody::Data(sections.iter().map(|section| section.to_vec()).collect())
            }
            Body::Sequence(sections) => MessageBody::Sequence(
                sections
                    .iter()
                    .map(|section| section.iter().map(read_value).collect())
                    .collect(),
            ),
            Body::Value(value) => MessageBody::Value(read_value(value)),
        },
    }
}

fn write_envelope(envelope: &MessageEnvelope) -> Message {
    Message {
        header: envelope.header.as_ref().map(|header| Header {
            durable: header.durable,
            priority: header.priority,
            first_acquirer: header.first_acquirer,
            ..Header::default()
        }),
        properties: Some(write_properties(&envelope.properties)),
        application_properties: Some(ApplicationProperties(
            envelope
                .application_properties
                .iter()
                .map(|(key, value)| (key.clone(), write_value(value)))
                .collect(),
        )),
        message_annotations: Some(write_annotations(&envelope.message_annotations)),
        footer: (!envelope.footer.is_empty()).then(|| write_annotations(&envelope.footer)),
        body: match &envelope.body {
            MessageBody::Empty => Body::Empty,
            MessageBody::Data(sections) => {
                Body::Data(sections.iter().cloned().map(Into::into).collect())
            }
            MessageBody::Sequence(sections) => Body::Sequence(
                sections
                    .iter()
                    .map(|section| section.iter().map(write_value).collect())
                    .collect(),
            ),
            MessageBody::Value(value) => Body::Value(write_value(value)),
        },
        // Delivery annotations are hop-local and deliberately not retained.
        ..Message::default()
    }
}

fn read_properties(properties: &Properties) -> MessageProperties {
    MessageProperties {
        message_id: properties.message_id.as_ref().map(read_identifier),
        correlation_id: properties.correlation_id.as_ref().map(read_identifier),
        user_id: properties.user_id.as_ref().map(|value| value.to_vec()),
        to: properties.to.clone(),
        subject: properties.subject.clone(),
        reply_to: properties.reply_to.clone(),
        content_type: properties
            .content_type
            .as_ref()
            .map(|value| value.as_str().to_owned()),
        content_encoding: properties
            .content_encoding
            .as_ref()
            .map(|value| value.as_str().to_owned()),
        creation_time: properties.creation_time,
        absolute_expiry_time: properties.absolute_expiry_time,
        group_sequence: properties.group_sequence,
        reply_to_group_id: properties.reply_to_group_id.clone(),
    }
}

fn write_properties(properties: &MessageProperties) -> Properties {
    Properties {
        message_id: properties.message_id.as_ref().map(write_identifier),
        correlation_id: properties.correlation_id.as_ref().map(write_identifier),
        user_id: properties.user_id.clone().map(Into::into),
        to: properties.to.clone(),
        subject: properties.subject.clone(),
        reply_to: properties.reply_to.clone(),
        content_type: properties.content_type.clone().map(Into::into),
        content_encoding: properties.content_encoding.clone().map(Into::into),
        creation_time: properties.creation_time,
        absolute_expiry_time: properties.absolute_expiry_time,
        group_sequence: properties.group_sequence,
        reply_to_group_id: properties.reply_to_group_id.clone(),
        ..Properties::default()
    }
}

fn read_identifier(identifier: &MessageId) -> MessageIdentifier {
    match identifier {
        MessageId::String(value) => MessageIdentifier::String(value.clone()),
        MessageId::Ulong(value) => MessageIdentifier::Ulong(*value),
        MessageId::Uuid(value) => MessageIdentifier::Uuid(*value.as_ref()),
        MessageId::Binary(value) => MessageIdentifier::Binary(value.to_vec()),
    }
}

fn write_identifier(identifier: &MessageIdentifier) -> MessageId {
    match identifier {
        MessageIdentifier::String(value) => MessageId::String(value.clone()),
        MessageIdentifier::Ulong(value) => MessageId::Ulong(*value),
        MessageIdentifier::Uuid(value) => MessageId::Uuid((*value).into()),
        MessageIdentifier::Binary(value) => MessageId::Binary(value.clone().into()),
    }
}

fn read_annotations(annotations: Option<&Annotations>) -> BTreeMap<AnnotationKey, MessageValue> {
    annotations
        .into_iter()
        .flat_map(Annotations::iter)
        .map(|(key, value)| {
            let key = match key {
                AmqpAnnotationKey::Symbol(value) => {
                    AnnotationKey::Symbol(value.as_str().to_owned())
                }
                AmqpAnnotationKey::Ulong(value) => AnnotationKey::Ulong(*value),
            };
            (key, read_value(value))
        })
        .collect()
}

fn write_annotations(annotations: &BTreeMap<AnnotationKey, MessageValue>) -> Annotations {
    Annotations(
        annotations
            .iter()
            .map(|(key, value)| {
                let key = match key {
                    AnnotationKey::Symbol(value) => AmqpAnnotationKey::Symbol(value.clone().into()),
                    AnnotationKey::Ulong(value) => AmqpAnnotationKey::Ulong(*value),
                };
                (key, write_value(value))
            })
            .collect(),
    )
}

pub(crate) fn read_value(value: &Value) -> MessageValue {
    match value {
        Value::Null => MessageValue::Null,
        Value::Bool(value) => MessageValue::Bool(*value),
        Value::Ubyte(value) => MessageValue::Ubyte(*value),
        Value::Ushort(value) => MessageValue::Ushort(*value),
        Value::Uint(value) => MessageValue::Uint(*value),
        Value::Ulong(value) => MessageValue::Ulong(*value),
        Value::Byte(value) => MessageValue::Byte(*value),
        Value::Short(value) => MessageValue::Short(*value),
        Value::Int(value) => MessageValue::Int(*value),
        Value::Long(value) => MessageValue::Long(*value),
        Value::Float(value) => MessageValue::Float(value.0.to_bits()),
        Value::Double(value) => MessageValue::Double(value.0.to_bits()),
        Value::Decimal32(value) => MessageValue::Decimal32(value.clone().into_inner()),
        Value::Decimal64(value) => MessageValue::Decimal64(value.clone().into_inner()),
        Value::Decimal128(value) => MessageValue::Decimal128(value.clone().into_inner()),
        Value::Char(value) => MessageValue::Char(*value),
        Value::Timestamp(value) => MessageValue::Timestamp(value.milliseconds()),
        Value::Uuid(value) => MessageValue::Uuid(*value.as_ref()),
        Value::Binary(value) => MessageValue::Binary(value.to_vec()),
        Value::String(value) => MessageValue::String(value.clone()),
        Value::Symbol(value) => MessageValue::Symbol(value.as_str().to_owned()),
        Value::List(value) => MessageValue::List(value.iter().map(read_value).collect()),
        Value::Map(value) => MessageValue::Map(
            value
                .iter()
                .map(|(key, value)| (read_value(key), read_value(value)))
                .collect(),
        ),
        Value::Array(value) => MessageValue::Array(value.iter().map(read_value).collect()),
        Value::Described(value) => MessageValue::Described {
            descriptor: match &value.descriptor {
                Descriptor::Code(value) => MessageDescriptor::Code(*value),
                Descriptor::Name(value) => MessageDescriptor::Name(value.as_str().to_owned()),
            },
            value: Box::new(read_value(&value.value)),
        },
    }
}

fn write_value(value: &MessageValue) -> Value {
    match value {
        MessageValue::Null => Value::Null,
        MessageValue::Bool(value) => Value::Bool(*value),
        MessageValue::Ubyte(value) => Value::Ubyte(*value),
        MessageValue::Ushort(value) => Value::Ushort(*value),
        MessageValue::Uint(value) => Value::Uint(*value),
        MessageValue::Ulong(value) => Value::Ulong(*value),
        MessageValue::Byte(value) => Value::Byte(*value),
        MessageValue::Short(value) => Value::Short(*value),
        MessageValue::Int(value) => Value::Int(*value),
        MessageValue::Long(value) => Value::Long(*value),
        MessageValue::Float(value) => Value::Float(f32::from_bits(*value).into()),
        MessageValue::Double(value) => Value::Double(f64::from_bits(*value).into()),
        MessageValue::Decimal32(value) => Value::Decimal32((*value).into()),
        MessageValue::Decimal64(value) => Value::Decimal64((*value).into()),
        MessageValue::Decimal128(value) => Value::Decimal128((*value).into()),
        MessageValue::Char(value) => Value::Char(*value),
        MessageValue::Timestamp(value) => Value::Timestamp((*value).into()),
        MessageValue::Uuid(value) => Value::Uuid((*value).into()),
        MessageValue::Binary(value) => Value::Binary(value.clone().into()),
        MessageValue::String(value) => Value::String(value.clone()),
        MessageValue::Symbol(value) => Value::Symbol(value.clone().into()),
        MessageValue::List(value) => Value::List(value.iter().map(write_value).collect()),
        MessageValue::Map(value) => Value::Map(
            value
                .iter()
                .map(|(key, value)| (write_value(key), write_value(value)))
                .collect(),
        ),
        MessageValue::Array(value) => Value::Array(Array::from(
            value.iter().map(write_value).collect::<Vec<_>>(),
        )),
        MessageValue::Described { descriptor, value } => Value::Described(Box::new(Described {
            descriptor: match descriptor {
                MessageDescriptor::Code(value) => Descriptor::Code(*value),
                MessageDescriptor::Name(value) => Descriptor::Name(value.clone().into()),
            },
            value: write_value(value),
        })),
    }
}

/// Compatibility bytes for the legacy domain body field. The envelope retains
/// every original section, including sequence and value bodies.
fn body_bytes(body: &Body) -> Vec<u8> {
    match body {
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
    use amqp::{decode_message, encode_message};
    use domain::{
        DeadLetterInfo, DeadLetterReason, DeliveryLock, LockToken, SequenceNumber, Timestamp,
    };

    use super::*;

    fn sent(properties: Option<Properties>, body: Vec<u8>) -> Message {
        let mut message = Message::data(body);
        message.properties = properties;
        message
    }

    fn delivery(envelope: Option<MessageEnvelope>) -> Delivery {
        Delivery {
            sequence: SequenceNumber::new(7),
            message_id: String::from("legacy-id"),
            body: b"legacy payload".to_vec(),
            enqueued_at: Timestamp::from_millis(10),
            expires_at: None,
            time_to_live_millis: None,
            envelope: envelope.map(Box::new),
            delivery_count: 3,
            lock: None,
            session_id: None,
            dead_letter: None,
            status: MessageStatus::Active,
            scheduled_enqueue_time: None,
        }
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
        // The sequence number identifies an anonymous message afterwards.
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
            status: MessageStatus::Active,
            scheduled_enqueue_time: None,
            expires_at: None,
            time_to_live_millis: None,
            envelope: None,
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

    #[test]
    fn a_scheduled_timestamp_crosses_and_malformed_timestamps_are_refused() {
        let mut message = Message::data(Vec::new());
        let mut annotations = Annotations::new();
        annotations.insert(
            Symbol::from(SCHEDULED_ENQUEUE_TIME_ANNOTATION),
            Value::Timestamp(12_345_i64.into()),
        );
        message.message_annotations = Some(annotations.clone());
        assert_eq!(
            read_incoming(&message)
                .expect("a valid scheduled message")
                .scheduled_enqueue_time,
            Some(Timestamp::from_millis(12_345))
        );
        for value in [
            Value::Timestamp((-1_i64).into()),
            Value::Long(12_345),
            Value::Null,
        ] {
            annotations.insert(Symbol::from(SCHEDULED_ENQUEUE_TIME_ANNOTATION), value);
            message.message_annotations = Some(annotations.clone());
            assert_eq!(
                read_incoming(&message),
                Err(ProtocolError::InvalidScheduledEnqueueTime)
            );
        }
    }

    #[test]
    fn every_message_value_keeps_its_type_and_bits_across_the_wire() {
        let values = vec![
            MessageValue::Null,
            MessageValue::Bool(true),
            MessageValue::Ubyte(u8::MAX),
            MessageValue::Ushort(u16::MAX),
            MessageValue::Uint(u32::MAX),
            MessageValue::Ulong(u64::MAX),
            MessageValue::Byte(i8::MIN),
            MessageValue::Short(i16::MIN),
            MessageValue::Int(i32::MIN),
            MessageValue::Long(i64::MIN),
            MessageValue::Float(0x7fc1_2345),
            MessageValue::Float((-0.0_f32).to_bits()),
            MessageValue::Double(0xfff8_0123_4567_89ab),
            MessageValue::Double((-0.0_f64).to_bits()),
            MessageValue::Decimal32([1, 2, 3, 4]),
            MessageValue::Decimal64([2; 8]),
            MessageValue::Decimal128([3; 16]),
            MessageValue::Char('\u{1f600}'),
            MessageValue::Timestamp(-123),
            MessageValue::Uuid([4; 16]),
            MessageValue::Binary(vec![0, 255]),
            MessageValue::String(String::new()),
            MessageValue::Symbol("custom".to_owned()),
            MessageValue::List(vec![MessageValue::Null, MessageValue::Long(-7)]),
            MessageValue::Map(vec![
                (MessageValue::String("z".to_owned()), MessageValue::Uint(2)),
                (MessageValue::String("a".to_owned()), MessageValue::Uint(1)),
            ]),
            MessageValue::Array(vec![MessageValue::Int(2), MessageValue::Int(-1)]),
            MessageValue::Described {
                descriptor: MessageDescriptor::Code(123),
                value: Box::new(MessageValue::List(vec![MessageValue::String(
                    "v".to_owned(),
                )])),
            },
            MessageValue::Described {
                descriptor: MessageDescriptor::Name("com.microsoft:uri".to_owned()),
                value: Box::new(MessageValue::String(
                    "https://example.org/messages/1".to_owned(),
                )),
            },
            MessageValue::Described {
                descriptor: MessageDescriptor::Name("com.microsoft:datetime-offset".to_owned()),
                value: Box::new(MessageValue::Long(638_000_000_000_000_000)),
            },
            MessageValue::Described {
                descriptor: MessageDescriptor::Name("com.microsoft:timespan".to_owned()),
                value: Box::new(MessageValue::Long(-12_345)),
            },
        ];
        let envelope = MessageEnvelope {
            application_properties: values
                .iter()
                .enumerate()
                .filter(|(_, value)| match value {
                    MessageValue::List(_) | MessageValue::Map(_) | MessageValue::Array(_) => false,
                    MessageValue::Described { value, .. } => !matches!(
                        value.as_ref(),
                        MessageValue::List(_) | MessageValue::Map(_) | MessageValue::Array(_)
                    ),
                    _ => true,
                })
                .map(|(index, value)| (format!("property-{index}"), value.clone()))
                .collect(),
            body: MessageBody::Value(MessageValue::List(values)),
            ..MessageEnvelope::default()
        };
        let encoded = encode_message(&write_envelope(&envelope)).expect("encodable content");
        let decoded = decode_message(&encoded).expect("valid message sections");
        assert_eq!(read_envelope(&decoded), envelope);
    }

    #[test]
    fn properties_annotations_and_footer_survive_but_delivery_annotations_do_not() {
        let mut message = Message::data(Vec::new());
        message.header = Some(Header {
            durable: true,
            priority: 8,
            first_acquirer: true,
            ..Header::default()
        });
        message.properties = Some(Properties {
            message_id: Some(MessageId::Binary(vec![1, 2, 3].into())),
            correlation_id: Some(MessageId::Uuid([7; 16].into())),
            user_id: Some(vec![0, 255].into()),
            to: Some("destination".to_owned()),
            subject: Some(String::new()),
            reply_to: Some("replies".to_owned()),
            content_type: Some("application/json".into()),
            content_encoding: Some("utf-8".into()),
            creation_time: Some(-1234),
            group_sequence: Some(99),
            reply_to_group_id: Some("reply-session".to_owned()),
            ..Properties::default()
        });
        let mut annotations = Annotations::new();
        annotations.insert(
            Symbol::from("custom"),
            Value::String("annotation".to_owned()),
        );
        annotations.insert(42_u64, Value::Int(7));
        message.message_annotations = Some(annotations.clone());
        message.footer = Some(annotations.clone());
        message.delivery_annotations = Some(annotations);

        let incoming = read_incoming(&message).expect("valid properties");
        let encoded = encode_message(&write_delivery(&delivery(Some(incoming.envelope))))
            .expect("encodable delivery");
        let outgoing = decode_message(&encoded).expect("valid delivery");
        assert_eq!(outgoing.properties, message.properties);
        assert_eq!(outgoing.footer, message.footer);
        assert_eq!(outgoing.delivery_annotations, None);
        let header = outgoing.header.expect("broker header");
        assert!(header.durable);
        assert_eq!(header.priority, 8);
        assert!(!header.first_acquirer);
        assert_eq!(header.delivery_count, 3);
        let annotations = outgoing.message_annotations.expect("delivery annotations");
        assert_eq!(annotations.get(42_u64), Some(&Value::Int(7)));
        assert_eq!(
            annotations.get(Symbol::from("custom")),
            Some(&Value::String("annotation".to_owned()))
        );
    }

    #[test]
    fn every_body_shape_and_section_boundary_survives_delivery() {
        for body in [
            MessageBody::Empty,
            MessageBody::Data(vec![vec![1, 2], Vec::new(), vec![3]]),
            MessageBody::Sequence(vec![
                vec![MessageValue::String("first".to_owned())],
                Vec::new(),
                vec![MessageValue::Null, MessageValue::Ulong(3)],
            ]),
            MessageBody::Value(MessageValue::Null),
            MessageBody::Value(MessageValue::Map(vec![(
                MessageValue::String("key".to_owned()),
                MessageValue::List(vec![MessageValue::Int(2)]),
            )])),
        ] {
            let envelope = MessageEnvelope {
                body: body.clone(),
                ..MessageEnvelope::default()
            };
            let encoded =
                encode_message(&write_delivery(&delivery(Some(envelope)))).expect("encodable body");
            let decoded = decode_message(&encoded).expect("valid body");
            assert_eq!(
                read_incoming(&decoded)
                    .expect("valid incoming")
                    .envelope
                    .body,
                body
            );
        }
    }

    #[test]
    fn absent_empty_and_typed_identifiers_remain_distinct() {
        for identifier in [
            None,
            Some(MessageIdentifier::String(String::new())),
            Some(MessageIdentifier::Binary(Vec::new())),
            Some(MessageIdentifier::Ulong(0)),
            Some(MessageIdentifier::Uuid([0; 16])),
        ] {
            let envelope = MessageEnvelope {
                properties: MessageProperties {
                    message_id: identifier.clone(),
                    correlation_id: identifier.clone(),
                    ..MessageProperties::default()
                },
                ..MessageEnvelope::default()
            };
            let wire = write_delivery(&delivery(Some(envelope)));
            let incoming = read_incoming(&wire).expect("valid identifier");
            assert_eq!(incoming.envelope.properties.message_id, identifier);
            assert_eq!(incoming.envelope.properties.correlation_id, identifier);
        }
    }

    #[test]
    fn effective_lifetimes_cross_the_header_limit_without_truncation() {
        let lifetime = u64::from(u32::MAX) + 123;
        let mut message = Message::data(Vec::new());
        message.header = Some(Header {
            ttl: Some(u32::MAX),
            ..Header::default()
        });
        message.properties = Some(Properties {
            creation_time: Some(-100),
            absolute_expiry_time: Some(i64::try_from(lifetime).expect("small lifetime") - 100),
            ..Properties::default()
        });
        let incoming = read_incoming(&message).expect("valid long lifetime");
        assert_eq!(incoming.time_to_live_millis, Some(lifetime));
        let mut delivery = delivery(Some(incoming.envelope));
        delivery.time_to_live_millis = Some(lifetime);
        delivery.expires_at = Some(Timestamp::from_millis(10 + lifetime));
        let outgoing = write_delivery(&delivery);
        assert_eq!(
            outgoing.header.as_ref().and_then(|header| header.ttl),
            Some(u32::MAX)
        );
        assert_eq!(
            outgoing
                .properties
                .as_ref()
                .and_then(|properties| properties.creation_time),
            Some(10)
        );
        assert_eq!(
            outgoing
                .properties
                .as_ref()
                .and_then(|properties| properties.absolute_expiry_time),
            Some(i64::try_from(10 + lifetime).expect("small expiry"))
        );
        assert_eq!(
            read_incoming(&outgoing)
                .expect("valid long delivery")
                .time_to_live_millis,
            Some(lifetime)
        );
    }

    #[test]
    fn pending_schedules_keep_their_lifetime_without_a_broker_deadline() {
        let lifetime = u64::from(u32::MAX) + 100;
        let mut delivery = delivery(Some(MessageEnvelope::default()));
        delivery.status = MessageStatus::Scheduled;
        delivery.scheduled_enqueue_time = Some(Timestamp::from_millis(1000));
        delivery.time_to_live_millis = Some(lifetime);
        let wire = write_delivery(&delivery);
        assert_eq!(
            read_incoming(&wire)
                .expect("pending lifetime")
                .time_to_live_millis,
            Some(lifetime)
        );
        assert_eq!(
            wire.properties
                .as_ref()
                .and_then(|properties| properties.creation_time),
            Some(1000)
        );

        let envelope = delivery.envelope.as_mut().expect("rich content");
        envelope.properties.creation_time = Some(123);
        envelope.properties.absolute_expiry_time = Some(173);
        delivery.time_to_live_millis = Some(50);
        let wire = write_delivery(&delivery);
        assert_eq!(wire.header.as_ref().and_then(|header| header.ttl), Some(50));
        assert_eq!(
            wire.properties
                .as_ref()
                .and_then(|properties| properties.creation_time),
            Some(123)
        );
        assert_eq!(
            wire.properties
                .as_ref()
                .and_then(|properties| properties.absolute_expiry_time),
            Some(173)
        );
    }

    #[test]
    fn broker_metadata_and_dead_letter_fields_override_spoofed_content() {
        let mut message = Message::data(b"retained".to_vec());
        message.header = Some(Header {
            ttl: Some(10),
            delivery_count: 99,
            ..Header::default()
        });
        message.properties = Some(Properties {
            creation_time: Some(100),
            absolute_expiry_time: Some(110),
            group_id: Some("old-session".to_owned()),
            ..Properties::default()
        });
        let mut annotations = Annotations::new();
        for key in [
            SEQUENCE_NUMBER_ANNOTATION,
            ENQUEUED_TIME_ANNOTATION,
            MESSAGE_STATE_ANNOTATION,
            LOCKED_UNTIL_ANNOTATION,
        ] {
            annotations.insert(Symbol::from(key), Value::Long(-1));
        }
        message.message_annotations = Some(annotations);
        let mut properties = ApplicationProperties::default();
        properties.insert("retained", "application value");
        properties.insert(DEAD_LETTER_REASON_PROPERTY, "spoofed");
        properties.insert(DEAD_LETTER_DESCRIPTION_PROPERTY, "spoofed");
        message.application_properties = Some(properties);
        let incoming = read_incoming(&message).expect("valid producer content");
        let mut delivery = delivery(Some(incoming.envelope));
        delivery.dead_letter = Some(DeadLetterInfo {
            reason: DeadLetterReason::Application("actual reason".to_owned()),
            description: "actual description".to_owned(),
            dead_lettered_at: Timestamp::from_millis(9),
        });
        let outgoing = write_delivery(&delivery);
        assert_eq!(outgoing.body, message.body);
        assert_eq!(outgoing.header.as_ref().and_then(|header| header.ttl), None);
        assert_eq!(
            outgoing.header.as_ref().map(|header| header.delivery_count),
            Some(3)
        );
        let properties = outgoing.properties.expect("properties retained");
        assert_eq!(properties.creation_time, Some(100));
        assert_eq!(properties.absolute_expiry_time, None);
        assert_eq!(properties.group_id, None);
        let annotations = outgoing.message_annotations.expect("broker annotations");
        assert_eq!(
            annotations.get(Symbol::from(SEQUENCE_NUMBER_ANNOTATION)),
            Some(&Value::Long(7))
        );
        assert_eq!(
            annotations.get(Symbol::from(ENQUEUED_TIME_ANNOTATION)),
            Some(&Value::Timestamp(10_i64.into()))
        );
        assert_eq!(
            annotations.get(Symbol::from(MESSAGE_STATE_ANNOTATION)),
            Some(&Value::Int(0))
        );
        assert_eq!(annotations.get(Symbol::from(LOCKED_UNTIL_ANNOTATION)), None);
        let properties = outgoing
            .application_properties
            .expect("application properties retained");
        assert_eq!(
            properties.get("retained"),
            Some(&Value::String("application value".to_owned()))
        );
        assert_eq!(
            properties.get(DEAD_LETTER_REASON_PROPERTY),
            Some(&Value::String("actual reason".to_owned()))
        );
        assert_eq!(
            properties.get(DEAD_LETTER_DESCRIPTION_PROPERTY),
            Some(&Value::String("actual description".to_owned()))
        );
    }
}
