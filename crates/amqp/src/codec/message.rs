use std::io;

use serde_amqp::{
    Value,
    primitives::{OrderedMap, Timestamp as AmqpTimestamp},
};

use super::{
    amqp_codec_error, append_value, binary_field, bool_field, described, encoded_value_len, field,
    invalid_data, list, optional_string, optional_u32, string_field, symbol_field, take_described,
    take_list, timestamp_field, u8_field, u32_field,
};
use crate::types::*;

const HEADER: u64 = 0x70;
const DELIVERY_ANNOTATIONS: u64 = 0x71;
const MESSAGE_ANNOTATIONS: u64 = 0x72;
const PROPERTIES: u64 = 0x73;
const APPLICATION_PROPERTIES: u64 = 0x74;
const DATA: u64 = 0x75;
const AMQP_SEQUENCE: u64 = 0x76;
const AMQP_VALUE: u64 = 0x77;
const FOOTER: u64 = 0x78;

pub fn encode_message(message: &Message) -> io::Result<Vec<u8>> {
    let mut encoded = Vec::new();
    if let Some(header) = &message.header {
        append_value(&mut encoded, header_to_value(header))?;
    }
    if let Some(annotations) = &message.delivery_annotations {
        append_value(
            &mut encoded,
            annotations_to_value(DELIVERY_ANNOTATIONS, &annotations.0),
        )?;
    }
    if let Some(annotations) = &message.message_annotations {
        append_value(
            &mut encoded,
            annotations_to_value(MESSAGE_ANNOTATIONS, &annotations.0),
        )?;
    }
    if let Some(properties) = &message.properties {
        append_value(&mut encoded, properties_to_value(properties))?;
    }
    if let Some(properties) = &message.application_properties {
        append_value(&mut encoded, application_properties_to_value(properties))?;
    }
    match &message.body {
        Body::Data(sections) => {
            if sections.is_empty() {
                return Err(invalid_data("data body has no sections"));
            }
            for section in sections {
                append_value(
                    &mut encoded,
                    described(DATA, Value::Binary(section.clone())),
                )?;
            }
        }
        Body::Sequence(sections) => {
            if sections.is_empty() {
                return Err(invalid_data("AMQP sequence body has no sections"));
            }
            for section in sections {
                append_value(
                    &mut encoded,
                    described(AMQP_SEQUENCE, Value::List(section.clone())),
                )?;
            }
        }
        Body::Value(value) => {
            append_value(&mut encoded, described(AMQP_VALUE, value.clone()))?;
        }
        Body::Empty => {}
    }
    if let Some(footer) = &message.footer {
        append_value(&mut encoded, annotations_to_value(FOOTER, &footer.0))?;
    }
    Ok(encoded)
}

pub fn decode_message(encoded: &[u8]) -> io::Result<Message> {
    let mut message = Message::default();
    let mut offset = 0;
    let mut last_section_rank = 0;
    while offset < encoded.len() {
        let len = encoded_value_len(&encoded[offset..])?;
        let value =
            serde_amqp::from_slice(&encoded[offset..offset + len]).map_err(amqp_codec_error)?;
        offset += len;

        let (descriptor, value) = take_described(value)?;
        let section_rank = message_section_rank(descriptor)?;
        if section_rank < last_section_rank {
            return Err(invalid_data("AMQP message sections are out of order"));
        }
        last_section_rank = section_rank;
        match descriptor {
            HEADER => {
                if message.header.is_some() {
                    return Err(invalid_data("duplicate AMQP header section"));
                }
                message.header = Some(header_from_value(value)?);
            }
            DELIVERY_ANNOTATIONS => {
                if message.delivery_annotations.is_some() {
                    return Err(invalid_data("duplicate delivery-annotations section"));
                }
                message.delivery_annotations = Some(DeliveryAnnotations(annotations_from_value(
                    value,
                    "delivery annotations",
                )?));
            }
            MESSAGE_ANNOTATIONS => {
                if message.message_annotations.is_some() {
                    return Err(invalid_data("duplicate message-annotations section"));
                }
                message.message_annotations = Some(MessageAnnotations(annotations_from_value(
                    value,
                    "message annotations",
                )?));
            }
            PROPERTIES => {
                if message.properties.is_some() {
                    return Err(invalid_data("duplicate AMQP properties section"));
                }
                message.properties = Some(properties_from_value(value)?);
            }
            APPLICATION_PROPERTIES => {
                if message.application_properties.is_some() {
                    return Err(invalid_data("duplicate application-properties section"));
                }
                message.application_properties = Some(application_properties_from_value(value)?);
            }
            DATA => match value {
                Value::Binary(section) => match &mut message.body {
                    Body::Empty => message.body = Body::Data(vec![section]),
                    Body::Data(sections) => sections.push(section),
                    Body::Sequence(_) | Body::Value(_) => {
                        return Err(invalid_data("mixed AMQP message body section types"));
                    }
                },
                _ => return Err(invalid_data("data section is not binary")),
            },
            AMQP_SEQUENCE => {
                let Value::List(sequence) = value else {
                    return Err(invalid_data("AMQP sequence body is not a list"));
                };
                match &mut message.body {
                    Body::Empty => message.body = Body::Sequence(vec![sequence]),
                    Body::Sequence(sections) => sections.push(sequence),
                    Body::Data(_) | Body::Value(_) => {
                        return Err(invalid_data("mixed AMQP message body section types"));
                    }
                }
            }
            AMQP_VALUE => match &message.body {
                Body::Empty => message.body = Body::Value(value),
                Body::Data(_) | Body::Sequence(_) | Body::Value(_) => {
                    return Err(invalid_data("duplicate or mixed AMQP body sections"));
                }
            },
            FOOTER => {
                if message.footer.is_some() {
                    return Err(invalid_data("duplicate AMQP footer section"));
                }
                message.footer = Some(Footer(annotations_from_value(value, "footer")?));
            }
            _ => unreachable!("message_section_rank rejected an unknown descriptor"),
        }
    }
    Ok(message)
}

fn message_section_rank(descriptor: u64) -> io::Result<u8> {
    match descriptor {
        HEADER => Ok(1),
        DELIVERY_ANNOTATIONS => Ok(2),
        MESSAGE_ANNOTATIONS => Ok(3),
        PROPERTIES => Ok(4),
        APPLICATION_PROPERTIES => Ok(5),
        DATA | AMQP_SEQUENCE | AMQP_VALUE => Ok(6),
        FOOTER => Ok(7),
        _ => Err(invalid_data(format!(
            "unknown AMQP message section descriptor {descriptor:#x}"
        ))),
    }
}

fn header_to_value(header: &Header) -> Value {
    described(
        HEADER,
        list(vec![
            Value::Bool(header.durable),
            Value::Ubyte(header.priority),
            optional_u32(header.ttl),
            Value::Bool(header.first_acquirer),
            Value::Uint(header.delivery_count),
        ]),
    )
}

fn header_from_value(value: Value) -> io::Result<Header> {
    let fields = take_list(value)?;
    Ok(Header {
        durable: bool_field(&fields, 0)?.unwrap_or(false),
        priority: u8_field(&fields, 1)?.unwrap_or(4),
        ttl: u32_field(&fields, 2)?,
        first_acquirer: bool_field(&fields, 3)?.unwrap_or(false),
        delivery_count: u32_field(&fields, 4)?.unwrap_or(0),
    })
}

fn properties_to_value(properties: &Properties) -> Value {
    described(
        PROPERTIES,
        list(vec![
            properties
                .message_id
                .as_ref()
                .map(message_id_to_value)
                .unwrap_or(Value::Null),
            properties
                .user_id
                .as_ref()
                .map(|value| Value::Binary(value.clone()))
                .unwrap_or(Value::Null),
            optional_string(&properties.to),
            optional_string(&properties.subject),
            optional_string(&properties.reply_to),
            properties
                .correlation_id
                .as_ref()
                .map(message_id_to_value)
                .unwrap_or(Value::Null),
            properties
                .content_type
                .as_ref()
                .map(|value| Value::Symbol(value.clone()))
                .unwrap_or(Value::Null),
            properties
                .content_encoding
                .as_ref()
                .map(|value| Value::Symbol(value.clone()))
                .unwrap_or(Value::Null),
            properties
                .absolute_expiry_time
                .map(|value| Value::Timestamp(AmqpTimestamp::from_milliseconds(value)))
                .unwrap_or(Value::Null),
            properties
                .creation_time
                .map(|value| Value::Timestamp(AmqpTimestamp::from_milliseconds(value)))
                .unwrap_or(Value::Null),
            optional_string(&properties.group_id),
            optional_u32(properties.group_sequence),
            optional_string(&properties.reply_to_group_id),
        ]),
    )
}

fn properties_from_value(value: Value) -> io::Result<Properties> {
    let fields = take_list(value)?;
    Ok(Properties {
        message_id: message_id_field(&fields, 0)?,
        user_id: binary_field(&fields, 1)?,
        to: string_field(&fields, 2)?,
        subject: string_field(&fields, 3)?,
        reply_to: string_field(&fields, 4)?,
        correlation_id: message_id_field(&fields, 5)?,
        content_type: symbol_field(&fields, 6)?,
        content_encoding: symbol_field(&fields, 7)?,
        absolute_expiry_time: timestamp_field(&fields, 8)?,
        creation_time: timestamp_field(&fields, 9)?,
        group_id: string_field(&fields, 10)?,
        group_sequence: u32_field(&fields, 11)?,
        reply_to_group_id: string_field(&fields, 12)?,
    })
}

fn application_properties_to_value(properties: &ApplicationProperties) -> Value {
    let mut map = OrderedMap::new();
    for (key, value) in properties.0.iter() {
        map.insert(Value::String(key.clone()), value.clone());
    }
    described(APPLICATION_PROPERTIES, Value::Map(map))
}

fn application_properties_from_value(value: Value) -> io::Result<ApplicationProperties> {
    let Value::Map(map) = value else {
        return Err(invalid_data("application properties are not a map"));
    };
    let mut properties = OrderedMap::new();
    for (key, value) in map {
        let Value::String(key) = key else {
            return Err(invalid_data("application property key is not a string"));
        };
        properties.insert(key, value);
    }
    Ok(ApplicationProperties(properties))
}

fn annotations_to_value(descriptor: u64, annotations: &OrderedMap<AnnotationKey, Value>) -> Value {
    let mut map = OrderedMap::new();
    for (key, value) in annotations.iter() {
        let key = match key {
            AnnotationKey::Symbol(key) => Value::Symbol(key.clone()),
            AnnotationKey::Ulong(key) => Value::Ulong(*key),
        };
        map.insert(key, value.clone());
    }
    described(descriptor, Value::Map(map))
}

fn annotations_from_value(
    value: Value,
    section_name: &str,
) -> io::Result<OrderedMap<AnnotationKey, Value>> {
    let Value::Map(map) = value else {
        return Err(invalid_data(format!("{section_name} are not a map")));
    };
    let mut annotations = OrderedMap::new();
    for (key, value) in map {
        let key = match key {
            Value::Symbol(key) => AnnotationKey::Symbol(key),
            Value::Ulong(key) => AnnotationKey::Ulong(key),
            _ => {
                return Err(invalid_data(format!(
                    "{section_name} key is not a symbol or ulong"
                )));
            }
        };
        annotations.insert(key, value);
    }
    Ok(annotations)
}

fn message_id_to_value(message_id: &MessageId) -> Value {
    match message_id {
        MessageId::Ulong(value) => Value::Ulong(*value),
        MessageId::Uuid(value) => Value::Uuid(value.clone()),
        MessageId::Binary(value) => Value::Binary(value.clone()),
        MessageId::String(value) => Value::String(value.clone()),
    }
}

fn message_id_field(fields: &[Value], index: usize) -> io::Result<Option<MessageId>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Ulong(value) => Some(MessageId::Ulong(value)),
        Value::Uuid(value) => Some(MessageId::Uuid(value)),
        Value::Binary(value) => Some(MessageId::Binary(value)),
        Value::String(value) => Some(MessageId::String(value)),
        _ => return Err(invalid_data("invalid AMQP message id")),
    })
}

#[cfg(test)]
mod tests {
    use serde_amqp::primitives::Binary;

    use super::*;

    #[test]
    fn message_sections_round_trip() {
        let mut application_properties = ApplicationProperties::default();
        application_properties.insert("status-code", 202_i32);
        application_properties.insert("status-description", String::from("Accepted"));
        let mut delivery_annotations = DeliveryAnnotations::default();
        delivery_annotations.insert("x-opt-delivery", String::from("delivery"));
        let mut message_annotations = MessageAnnotations::default();
        message_annotations.insert(7_u64, String::from("message"));
        let mut footer = Footer::default();
        footer.insert("checksum", Binary::from(vec![1, 2, 3]));
        let message = Message {
            header: Some(Header {
                ttl: Some(5000),
                ..Header::default()
            }),
            delivery_annotations: Some(delivery_annotations),
            message_annotations: Some(message_annotations),
            properties: Some(Properties {
                message_id: Some(MessageId::String(String::from("message-1"))),
                absolute_expiry_time: Some(9_876_543),
                creation_time: Some(-123),
                group_id: Some(String::from("cart-1")),
                ..Properties::default()
            }),
            application_properties: Some(application_properties),
            body: Body::Data(vec![
                Binary::from(b"one".to_vec()),
                Binary::from(b"two".to_vec()),
            ]),
            footer: Some(footer),
        };

        let encoded = encode_message(&message).expect("message encodes");
        assert_eq!(decode_message(&encoded).expect("message decodes"), message);
    }

    #[test]
    fn sequence_body_sections_round_trip_without_collapsing() {
        let message = Message::builder()
            .body(Body::Sequence(vec![
                vec![Value::String(String::from("first")), Value::Int(1)],
                vec![Value::String(String::from("second")), Value::Int(2)],
            ]))
            .build();

        let encoded = encode_message(&message).expect("message encodes");
        assert_eq!(decode_message(&encoded).expect("message decodes"), message);
    }

    #[test]
    fn property_times_are_encoded_as_amqp_timestamps() {
        let message = Message::builder()
            .properties(Properties {
                absolute_expiry_time: Some(10),
                creation_time: Some(-20),
                ..Properties::default()
            })
            .build();
        let encoded = encode_message(&message).expect("message encodes");
        let encoded_len = encoded_value_len(&encoded).expect("properties section has a length");
        let value: Value =
            serde_amqp::from_slice(&encoded[..encoded_len]).expect("properties section decodes");
        let (descriptor, value) = take_described(value).expect("properties are described");
        assert_eq!(descriptor, PROPERTIES);
        let fields = take_list(value).expect("properties use list encoding");
        assert!(matches!(field(&fields, 8), Value::Timestamp(value) if value.milliseconds() == 10));
        assert!(
            matches!(field(&fields, 9), Value::Timestamp(value) if value.milliseconds() == -20)
        );
    }

    #[test]
    fn message_decoder_rejects_duplicate_mixed_unknown_and_out_of_order_sections() {
        let mut duplicate = Vec::new();
        append_value(&mut duplicate, header_to_value(&Header::default())).unwrap();
        append_value(&mut duplicate, header_to_value(&Header::default())).unwrap();

        let mut mixed = Vec::new();
        append_value(
            &mut mixed,
            described(DATA, Value::Binary(Binary::from(vec![1]))),
        )
        .unwrap();
        append_value(
            &mut mixed,
            described(AMQP_SEQUENCE, Value::List(vec![Value::Int(2)])),
        )
        .unwrap();

        let mut unknown = Vec::new();
        append_value(&mut unknown, described(0x79, Value::Null)).unwrap();

        let mut out_of_order = Vec::new();
        append_value(
            &mut out_of_order,
            application_properties_to_value(&ApplicationProperties::default()),
        )
        .unwrap();
        append_value(
            &mut out_of_order,
            properties_to_value(&Properties::default()),
        )
        .unwrap();

        for invalid in [duplicate, mixed, unknown, out_of_order] {
            assert!(decode_message(&invalid).is_err());
        }
    }

    #[test]
    fn message_decoder_rejects_invalid_annotation_keys_and_long_timestamps() {
        let mut invalid_annotations = OrderedMap::new();
        invalid_annotations.insert(Value::String(String::from("not-a-symbol")), Value::Int(1));
        let invalid_annotations = serde_amqp::to_vec(&described(
            MESSAGE_ANNOTATIONS,
            Value::Map(invalid_annotations),
        ))
        .expect("invalid annotations encode as a generic AMQP value");
        assert!(decode_message(&invalid_annotations).is_err());

        let mut property_fields = vec![Value::Null; 10];
        property_fields[8] = Value::Long(10);
        let invalid_timestamp =
            serde_amqp::to_vec(&described(PROPERTIES, Value::List(property_fields)))
                .expect("invalid properties encode as a generic AMQP value");
        assert!(decode_message(&invalid_timestamp).is_err());
    }

    #[test]
    fn empty_sectioned_bodies_are_not_encodable() {
        let empty_data = Message::builder().body(Body::Data(Vec::new())).build();
        let empty_sequence = Message::builder().body(Body::Sequence(Vec::new())).build();
        assert!(encode_message(&empty_data).is_err());
        assert!(encode_message(&empty_sequence).is_err());
    }
}
