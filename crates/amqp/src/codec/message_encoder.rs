use std::io;

use crate::types::{AnnotationKey, Annotations, Body, Header, Message, MessageId, Properties};

use super::value_encoder::{Encoder, Field, measure, write_prepared};
use super::{
    AMQP_SEQUENCE, AMQP_VALUE, APPLICATION_PROPERTIES, DATA, DELIVERY_ANNOTATIONS, FOOTER, HEADER,
    MESSAGE_ANNOTATIONS, PROPERTIES,
};

#[derive(Debug, thiserror::Error)]
#[error("encoded AMQP message size {size} exceeds maximum {maximum}")]
pub struct MessageSizeError {
    pub size: usize,
    pub maximum: usize,
}

pub(crate) struct PreparedMessage<'a> {
    message: &'a Message,
    encoded_len: usize,
}

impl PreparedMessage<'_> {
    pub(crate) fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    pub(crate) fn encode(self) -> io::Result<Vec<u8>> {
        write_prepared(self.encoded_len, |encoder| {
            encode_sections(self.message, encoder)
        })
    }
}

pub(crate) fn prepare_message(message: &Message) -> io::Result<PreparedMessage<'_>> {
    let encoded_len = measure(|encoder| encode_sections(message, encoder))?;
    Ok(PreparedMessage {
        message,
        encoded_len,
    })
}

/// Encodes borrowed message sections after validating their exact wire size.
pub fn encode_message_with_max_size(message: &Message, maximum: usize) -> io::Result<Vec<u8>> {
    let prepared = prepare_message(message)?;
    if prepared.encoded_len() > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            MessageSizeError {
                size: prepared.encoded_len(),
                maximum,
            },
        ));
    }
    prepared.encode()
}

fn encode_sections(message: &Message, encoder: &mut Encoder) -> io::Result<()> {
    if let Some(header) = &message.header {
        encoder.described(HEADER, |encoder| encode_header(header, encoder))?;
    }
    if let Some(annotations) = &message.delivery_annotations {
        encoder.described(DELIVERY_ANNOTATIONS, |encoder| {
            encode_annotations(annotations, encoder)
        })?;
    }
    if let Some(annotations) = &message.message_annotations {
        encoder.described(MESSAGE_ANNOTATIONS, |encoder| {
            encode_annotations(annotations, encoder)
        })?;
    }
    if let Some(properties) = &message.properties {
        encoder.described(PROPERTIES, |encoder| encode_properties(properties, encoder))?;
    }
    if let Some(properties) = &message.application_properties {
        encoder.described(APPLICATION_PROPERTIES, |encoder| {
            encoder.map(
                properties.0.len(),
                properties
                    .0
                    .iter()
                    .map(|(key, value)| (Field::String(key), Field::Value(value))),
            )
        })?;
    }
    match &message.body {
        Body::Data(sections) => {
            for section in sections {
                encoder.described(DATA, |encoder| {
                    encoder.field(Field::Binary(section.as_ref()))
                })?;
            }
        }
        Body::Sequence(sections) => {
            for section in sections {
                encoder.described(AMQP_SEQUENCE, |encoder| {
                    encoder.list(section.len(), section.iter().map(Field::Value))
                })?;
            }
        }
        Body::Value(value) => {
            encoder.described(AMQP_VALUE, |encoder| encoder.value(value))?;
        }
        Body::Empty => {}
    }
    if let Some(footer) = &message.footer {
        encoder.described(FOOTER, |encoder| encode_annotations(footer, encoder))?;
    }
    Ok(())
}

fn encode_header(header: &Header, encoder: &mut Encoder) -> io::Result<()> {
    let fields = [
        Field::Bool(header.durable),
        Field::Ubyte(header.priority),
        optional_uint(header.ttl),
        Field::Bool(header.first_acquirer),
        Field::Uint(header.delivery_count),
    ];
    encode_fields(&fields, encoder)
}

fn encode_properties(properties: &Properties, encoder: &mut Encoder) -> io::Result<()> {
    let fields = [
        properties
            .message_id
            .as_ref()
            .map(message_id_field)
            .unwrap_or(Field::Null),
        properties
            .user_id
            .as_ref()
            .map(|value| Field::Binary(value.as_ref()))
            .unwrap_or(Field::Null),
        optional_string(properties.to.as_deref()),
        optional_string(properties.subject.as_deref()),
        optional_string(properties.reply_to.as_deref()),
        properties
            .correlation_id
            .as_ref()
            .map(message_id_field)
            .unwrap_or(Field::Null),
        properties
            .content_type
            .as_ref()
            .map(|value| Field::Symbol(value.as_str()))
            .unwrap_or(Field::Null),
        properties
            .content_encoding
            .as_ref()
            .map(|value| Field::Symbol(value.as_str()))
            .unwrap_or(Field::Null),
        properties
            .absolute_expiry_time
            .map(Field::Timestamp)
            .unwrap_or(Field::Null),
        properties
            .creation_time
            .map(Field::Timestamp)
            .unwrap_or(Field::Null),
        optional_string(properties.group_id.as_deref()),
        optional_uint(properties.group_sequence),
        optional_string(properties.reply_to_group_id.as_deref()),
    ];
    encode_fields(&fields, encoder)
}

fn encode_fields(fields: &[Field<'_>], encoder: &mut Encoder) -> io::Result<()> {
    let count = fields
        .iter()
        .rposition(|field| !matches!(field, Field::Null))
        .map_or(0, |last| last + 1);
    encoder.list(count, fields[..count].iter().copied())
}

fn encode_annotations(annotations: &Annotations, encoder: &mut Encoder) -> io::Result<()> {
    encoder.map(
        annotations.len(),
        annotations.iter().map(|(key, value)| {
            let key = match key {
                AnnotationKey::Symbol(key) => Field::Symbol(key.as_str()),
                AnnotationKey::Ulong(key) => Field::Ulong(*key),
            };
            (key, Field::Value(value))
        }),
    )
}

fn message_id_field(message_id: &MessageId) -> Field<'_> {
    match message_id {
        MessageId::Ulong(value) => Field::Ulong(*value),
        MessageId::Uuid(value) => Field::Uuid(value.as_ref()),
        MessageId::Binary(value) => Field::Binary(value.as_ref()),
        MessageId::String(value) => Field::String(value),
    }
}

fn optional_uint(value: Option<u32>) -> Field<'static> {
    value.map(Field::Uint).unwrap_or(Field::Null)
}

fn optional_string(value: Option<&str>) -> Field<'_> {
    value.map(Field::String).unwrap_or(Field::Null)
}
