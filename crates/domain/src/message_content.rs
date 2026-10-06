//! Protocol-neutral message content retained by the broker.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::BrokerError;

mod action_projection;
#[cfg(test)]
mod removal_projection;

/// Maximum child nesting for a retained value. Root values have depth zero;
/// every compound or described child increases the depth by one.
pub const MAX_MESSAGE_VALUE_DEPTH: usize = 64;

/// Maximum number of explicit value nodes across an envelope. Containers count
/// as values, including the top container; implicit sections and descriptors do
/// not. The wire decoder uses the same ceiling with its own section accounting.
pub const MAX_MESSAGE_VALUE_ITEMS: usize = 65_536;

/// Local conservative per-property content limit, including key overhead.
pub const MAX_MESSAGE_PROPERTY_BYTES: usize = 32 * 1024;

/// Local conservative brokered-header limit. Footer and body are excluded;
/// retained message annotations are included as a local policy.
pub const MAX_MESSAGE_HEADER_BYTES: usize = 64 * 1024;

// Conservative type, length, count, and descriptor overheads. These bound the
// retained content; they are not an exact AMQP wire-size calculation.
const VARIABLE_OVERHEAD: usize = 5;
const COLLECTION_OVERHEAD: usize = 9;
const SECTION_OVERHEAD: usize = 10;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MessageIdentifier {
    String(String),
    Ulong(u64),
    Uuid([u8; 16]),
    Binary(Vec<u8>),
}

impl MessageIdentifier {
    fn content_size(&self) -> usize {
        match self {
            Self::String(value) => VARIABLE_OVERHEAD.saturating_add(value.len()),
            Self::Ulong(_) => 9,
            Self::Uuid(_) => 17,
            Self::Binary(value) => VARIABLE_OVERHEAD.saturating_add(value.len()),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum AnnotationKey {
    Symbol(String),
    Ulong(u64),
}

impl AnnotationKey {
    fn content_size(&self) -> usize {
        match self {
            Self::Symbol(value) => VARIABLE_OVERHEAD.saturating_add(value.len()),
            Self::Ulong(_) => 9,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MessageDescriptor {
    Code(u64),
    Name(String),
}

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum MessageValue {
    #[default]
    Null,
    Bool(bool),
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    /// IEEE-754 bits, retaining NaN payloads and signed zero exactly.
    Float(u32),
    Double(u64),
    Decimal32([u8; 4]),
    Decimal64([u8; 8]),
    Decimal128([u8; 16]),
    Char(char),
    Timestamp(i64),
    Uuid([u8; 16]),
    Binary(Vec<u8>),
    String(String),
    Symbol(String),
    List(Vec<Self>),
    Map(Vec<(Self, Self)>),
    Array(Vec<Self>),
    Described {
        descriptor: MessageDescriptor,
        value: Box<Self>,
    },
}

impl MessageValue {
    fn has_same_constructor(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Described {
                    descriptor: left_descriptor,
                    value: left,
                },
                Self::Described {
                    descriptor: right_descriptor,
                    value: right,
                },
            ) => left_descriptor == right_descriptor && left.has_same_constructor(right),
            _ => std::mem::discriminant(self) == std::mem::discriminant(other),
        }
    }

    fn is_simple_type(&self) -> bool {
        match self {
            Self::List(_) | Self::Map(_) | Self::Array(_) => false,
            Self::Described { value, .. } => value.is_simple_type(),
            _ => true,
        }
    }

    fn canonical_map_key(&self) -> Self {
        let mut key = self.clone();
        let mut pending = vec![&mut key];
        while let Some(value) = pending.pop() {
            match value {
                Self::Float(bits) => {
                    let value = f32::from_bits(*bits);
                    if value == 0.0 {
                        *bits = 0;
                    } else if value.is_nan() {
                        *bits = 0x7fc0_0000;
                    }
                }
                Self::Double(bits) => {
                    let value = f64::from_bits(*bits);
                    if value == 0.0 {
                        *bits = 0;
                    } else if value.is_nan() {
                        *bits = 0x7ff8_0000_0000_0000;
                    }
                }
                Self::List(values) | Self::Array(values) => pending.extend(values.iter_mut()),
                Self::Map(entries) => {
                    pending.extend(entries.iter_mut().flat_map(|(key, value)| [key, value]))
                }
                Self::Described { value, .. } => pending.push(value),
                _ => {}
            }
        }
        key
    }

    pub fn content_size(&self) -> usize {
        let mut pending = vec![self];
        let mut size = 0_usize;
        while let Some(value) = pending.pop() {
            let own_size = match value {
                Self::Null => 1,
                Self::Bool(_) | Self::Ubyte(_) | Self::Byte(_) => 2,
                Self::Ushort(_) | Self::Short(_) => 3,
                Self::Uint(_)
                | Self::Int(_)
                | Self::Float(_)
                | Self::Decimal32(_)
                | Self::Char(_) => 5,
                Self::Ulong(_)
                | Self::Long(_)
                | Self::Double(_)
                | Self::Decimal64(_)
                | Self::Timestamp(_) => 9,
                Self::Decimal128(_) | Self::Uuid(_) => 17,
                Self::Binary(value) => VARIABLE_OVERHEAD.saturating_add(value.len()),
                Self::String(value) | Self::Symbol(value) => {
                    VARIABLE_OVERHEAD.saturating_add(value.len())
                }
                Self::List(values) | Self::Array(values) => {
                    pending.extend(values);
                    COLLECTION_OVERHEAD
                }
                Self::Map(entries) => {
                    pending.extend(entries.iter().flat_map(|(key, value)| [key, value]));
                    COLLECTION_OVERHEAD
                }
                Self::Described { descriptor, value } => {
                    pending.push(value);
                    1_usize.saturating_add(match descriptor {
                        MessageDescriptor::Code(_) => 9,
                        MessageDescriptor::Name(value) => {
                            VARIABLE_OVERHEAD.saturating_add(value.len())
                        }
                    })
                }
            };
            size = size.saturating_add(own_size);
        }
        size
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum MessageBody {
    #[default]
    Empty,
    Data(Vec<Vec<u8>>),
    Sequence(Vec<Vec<MessageValue>>),
    Value(MessageValue),
}

impl MessageBody {
    fn content_size(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Data(sections) => sections.iter().fold(0_usize, |size, section| {
                size.saturating_add(SECTION_OVERHEAD)
                    .saturating_add(VARIABLE_OVERHEAD)
                    .saturating_add(section.len())
            }),
            Self::Sequence(sections) => sections.iter().fold(0_usize, |size, section| {
                section.iter().fold(
                    size.saturating_add(SECTION_OVERHEAD)
                        .saturating_add(COLLECTION_OVERHEAD),
                    |size, value| size.saturating_add(value.content_size()),
                )
            }),
            Self::Value(value) => SECTION_OVERHEAD.saturating_add(value.content_size()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageHeader {
    pub durable: bool,
    pub priority: u8,
    pub first_acquirer: bool,
}

impl Default for MessageHeader {
    fn default() -> Self {
        Self {
            durable: false,
            priority: 4,
            first_acquirer: false,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageProperties {
    pub message_id: Option<MessageIdentifier>,
    pub correlation_id: Option<MessageIdentifier>,
    pub user_id: Option<Vec<u8>>,
    pub to: Option<String>,
    pub subject: Option<String>,
    pub reply_to: Option<String>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub reply_to_group_id: Option<String>,
    pub creation_time: Option<i64>,
    pub absolute_expiry_time: Option<i64>,
    pub group_sequence: Option<u32>,
}

impl MessageProperties {
    fn validate_property_sizes(&self) -> Result<(), BrokerError> {
        let sizes = [
            (
                "message-id",
                self.message_id
                    .as_ref()
                    .map(MessageIdentifier::content_size),
            ),
            (
                "correlation-id",
                self.correlation_id
                    .as_ref()
                    .map(MessageIdentifier::content_size),
            ),
            (
                "user-id",
                self.user_id
                    .as_ref()
                    .map(|value| VARIABLE_OVERHEAD.saturating_add(value.len())),
            ),
            ("to", self.to.as_deref().map(string_content_size)),
            ("subject", self.subject.as_deref().map(string_content_size)),
            (
                "reply-to",
                self.reply_to.as_deref().map(string_content_size),
            ),
            (
                "content-type",
                self.content_type.as_deref().map(string_content_size),
            ),
            (
                "content-encoding",
                self.content_encoding.as_deref().map(string_content_size),
            ),
            (
                "reply-to-group-id",
                self.reply_to_group_id.as_deref().map(string_content_size),
            ),
            ("creation-time", self.creation_time.map(|_| 9)),
            ("absolute-expiry-time", self.absolute_expiry_time.map(|_| 9)),
            ("group-sequence", self.group_sequence.map(|_| 5)),
        ];
        for (name, value_bytes) in sizes {
            if let Some(value_bytes) = value_bytes {
                validate_property_size(
                    name,
                    VARIABLE_OVERHEAD.saturating_add(name.len()),
                    value_bytes,
                )?;
            }
        }
        Ok(())
    }

    fn content_size(&self) -> usize {
        let mut size = SECTION_OVERHEAD.saturating_add(COLLECTION_OVERHEAD);
        for value in [&self.message_id, &self.correlation_id] {
            size = size.saturating_add(value.as_ref().map_or(1, MessageIdentifier::content_size));
        }
        size = size.saturating_add(
            self.user_id
                .as_ref()
                .map_or(1, |value| VARIABLE_OVERHEAD.saturating_add(value.len())),
        );
        for value in [
            &self.to,
            &self.subject,
            &self.reply_to,
            &self.content_type,
            &self.content_encoding,
            &self.reply_to_group_id,
        ] {
            size = size.saturating_add(
                value
                    .as_ref()
                    .map_or(1, |value| VARIABLE_OVERHEAD.saturating_add(value.len())),
            );
        }
        size = size.saturating_add(self.creation_time.map_or(1, |_| 9));
        size = size.saturating_add(self.absolute_expiry_time.map_or(1, |_| 9));
        size.saturating_add(self.group_sequence.map_or(1, |_| 5))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub header: Option<MessageHeader>,
    pub properties: MessageProperties,
    pub application_properties: BTreeMap<String, MessageValue>,
    pub message_annotations: BTreeMap<AnnotationKey, MessageValue>,
    pub footer: BTreeMap<AnnotationKey, MessageValue>,
    pub body: MessageBody,
}

impl MessageEnvelope {
    /// Rejects content that cannot round-trip through the protocol value model.
    /// Float map keys follow the edge codec's map-key equality: signed zero and
    /// all NaNs compare equal, without changing retained producer value bits.
    /// Valid shapes then pass the local conservative property/header quotas.
    pub fn validate(&self) -> Result<(), BrokerError> {
        self.validate_value_limits()?;
        let invalid = |reason: &str| BrokerError::InvalidMessageContent {
            reason: reason.to_owned(),
        };
        for symbol in [
            &self.properties.content_type,
            &self.properties.content_encoding,
        ]
        .into_iter()
        .flatten()
        {
            if !symbol.is_ascii() {
                return Err(invalid("content type and encoding require ASCII symbols"));
            }
        }
        for key in self.message_annotations.keys().chain(self.footer.keys()) {
            if matches!(key, AnnotationKey::Symbol(symbol) if !symbol.is_ascii()) {
                return Err(invalid("annotation keys require ASCII symbols"));
            }
        }
        for value in self.application_properties.values() {
            if !value.is_simple_type() {
                return Err(invalid("application properties require simple values"));
            }
        }
        let mut pending = self.application_properties.values().collect::<Vec<_>>();
        pending.extend(self.message_annotations.values());
        pending.extend(self.footer.values());
        match &self.body {
            MessageBody::Empty | MessageBody::Data(_) => {}
            MessageBody::Sequence(sections) => pending.extend(sections.iter().flatten()),
            MessageBody::Value(value) => pending.push(value),
        }
        while let Some(value) = pending.pop() {
            match value {
                MessageValue::Array(values) => {
                    let Some(first) = values.first() else {
                        return Err(invalid("empty arrays do not retain an element constructor"));
                    };
                    if values
                        .iter()
                        .any(|value| !first.has_same_constructor(value))
                    {
                        return Err(invalid("array elements require the same constructor"));
                    }
                    pending.extend(values);
                }
                MessageValue::List(values) => pending.extend(values),
                MessageValue::Map(entries) => {
                    let mut keys = BTreeSet::new();
                    for (key, value) in entries {
                        if !keys.insert(key.canonical_map_key()) {
                            return Err(invalid("map keys must be unique"));
                        }
                        pending.push(key);
                        pending.push(value);
                    }
                }
                MessageValue::Described { descriptor, value } => {
                    if matches!(descriptor, MessageDescriptor::Name(symbol) if !symbol.is_ascii()) {
                        return Err(invalid("symbolic descriptors require ASCII symbols"));
                    }
                    pending.push(value);
                }
                MessageValue::Symbol(symbol) if !symbol.is_ascii() => {
                    return Err(invalid("symbol values require ASCII symbols"));
                }
                _ => {}
            }
        }
        self.validate_property_quotas()
    }

    pub(crate) fn validate_application_property_updates(
        properties: &BTreeMap<String, MessageValue>,
    ) -> Result<(), BrokerError> {
        let invalid = |reason: &str| BrokerError::InvalidMessageContent {
            reason: reason.to_owned(),
        };
        let mut pending = Vec::new();
        let mut items = 0;
        push_value_nodes(&mut pending, properties.values(), 0, &mut items)?;
        while let Some((value, depth)) = pending.pop() {
            match value {
                MessageValue::List(_) | MessageValue::Map(_) | MessageValue::Array(_) => {
                    return Err(invalid("application properties require simple values"));
                }
                MessageValue::Described { descriptor, value } => {
                    if matches!(descriptor, MessageDescriptor::Name(symbol) if !symbol.is_ascii()) {
                        return Err(invalid("symbolic descriptors require ASCII symbols"));
                    }
                    push_value_nodes(
                        &mut pending,
                        std::iter::once(value.as_ref()),
                        depth + 1,
                        &mut items,
                    )?;
                }
                MessageValue::Symbol(symbol) if !symbol.is_ascii() => {
                    return Err(invalid("symbol values require ASCII symbols"));
                }
                _ => {}
            }
        }
        let mut header_bytes = if properties.is_empty() {
            0
        } else {
            SECTION_OVERHEAD.saturating_add(COLLECTION_OVERHEAD)
        };
        for (key, value) in properties {
            let key_bytes = VARIABLE_OVERHEAD.saturating_add(key.len());
            let value_bytes = value.content_size();
            validate_property_size(key, key_bytes, value_bytes)?;
            header_bytes = header_bytes
                .saturating_add(key_bytes)
                .saturating_add(value_bytes);
            if header_bytes > MAX_MESSAGE_HEADER_BYTES {
                return Err(BrokerError::MessageHeaderTooLarge {
                    header_bytes,
                    maximum_bytes: MAX_MESSAGE_HEADER_BYTES,
                });
            }
        }
        Ok(())
    }

    pub(crate) fn validate_property_quotas(&self) -> Result<(), BrokerError> {
        self.properties.validate_property_sizes()?;
        for (key, value) in &self.application_properties {
            validate_property_size(
                key,
                VARIABLE_OVERHEAD.saturating_add(key.len()),
                value.content_size(),
            )?;
        }
        for (key, value) in &self.message_annotations {
            let name = match key {
                AnnotationKey::Symbol(symbol) => symbol.clone(),
                AnnotationKey::Ulong(code) => format!("annotation[{code}]"),
            };
            validate_property_size(&name, key.content_size(), value.content_size())?;
        }
        let header_bytes = self.header_content_size();
        if header_bytes > MAX_MESSAGE_HEADER_BYTES {
            return Err(BrokerError::MessageHeaderTooLarge {
                header_bytes,
                maximum_bytes: MAX_MESSAGE_HEADER_BYTES,
            });
        }
        Ok(())
    }

    pub(crate) fn validate_value_limits(&self) -> Result<usize, BrokerError> {
        let mut pending = Vec::new();
        let mut items = 0;
        push_value_nodes(
            &mut pending,
            self.application_properties.values(),
            0,
            &mut items,
        )?;
        push_value_nodes(
            &mut pending,
            self.message_annotations.values(),
            0,
            &mut items,
        )?;
        push_value_nodes(&mut pending, self.footer.values(), 0, &mut items)?;
        match &self.body {
            MessageBody::Empty | MessageBody::Data(_) => {}
            MessageBody::Sequence(sections) => {
                push_value_nodes(&mut pending, sections.iter().flatten(), 0, &mut items)?;
            }
            MessageBody::Value(value) => {
                push_value_nodes(&mut pending, std::iter::once(value), 0, &mut items)?;
            }
        }
        while let Some((value, depth)) = pending.pop() {
            match value {
                MessageValue::List(values) | MessageValue::Array(values) => {
                    push_value_nodes(&mut pending, values.iter(), depth + 1, &mut items)?;
                }
                MessageValue::Map(entries) => {
                    push_value_nodes(
                        &mut pending,
                        entries.iter().flat_map(|(key, value)| [key, value]),
                        depth + 1,
                        &mut items,
                    )?;
                }
                MessageValue::Described { value, .. } => {
                    push_value_nodes(
                        &mut pending,
                        std::iter::once(value.as_ref()),
                        depth + 1,
                        &mut items,
                    )?;
                }
                _ => {}
            }
        }
        Ok(items)
    }

    /// Conservative retained header tally, not an exact wire calculation.
    /// Property names are diagnostic-only for positional standard properties;
    /// application and annotation keys contribute their encoded content sizes.
    pub fn header_content_size(&self) -> usize {
        let mut size = self.properties.content_size();
        if self.header.is_some() {
            size = size
                .saturating_add(SECTION_OVERHEAD)
                .saturating_add(COLLECTION_OVERHEAD)
                .saturating_add(6);
        }
        if !self.application_properties.is_empty() {
            size = size
                .saturating_add(SECTION_OVERHEAD)
                .saturating_add(COLLECTION_OVERHEAD);
            for (key, value) in &self.application_properties {
                size = size
                    .saturating_add(VARIABLE_OVERHEAD)
                    .saturating_add(key.len())
                    .saturating_add(value.content_size());
            }
        }
        if !self.message_annotations.is_empty() {
            size = size
                .saturating_add(SECTION_OVERHEAD)
                .saturating_add(COLLECTION_OVERHEAD);
            for (key, value) in &self.message_annotations {
                size = size
                    .saturating_add(key.content_size())
                    .saturating_add(value.content_size());
            }
        }
        size
    }

    /// A conservative content tally, including retained metadata and section
    /// boundaries, rather than the exact size of any protocol's encoding.
    pub fn content_size(&self) -> usize {
        let mut size = self
            .header_content_size()
            .saturating_add(self.body.content_size());
        if !self.footer.is_empty() {
            size = size
                .saturating_add(SECTION_OVERHEAD)
                .saturating_add(COLLECTION_OVERHEAD);
            for (key, value) in &self.footer {
                size = size
                    .saturating_add(key.content_size())
                    .saturating_add(value.content_size());
            }
        }
        size
    }
}

fn string_content_size(value: &str) -> usize {
    VARIABLE_OVERHEAD.saturating_add(value.len())
}

fn validate_property_size(
    property: &str,
    key_bytes: usize,
    value_bytes: usize,
) -> Result<(), BrokerError> {
    let property_bytes = key_bytes.saturating_add(value_bytes);
    if property_bytes > MAX_MESSAGE_PROPERTY_BYTES {
        return Err(BrokerError::MessagePropertyTooLarge {
            property: property.to_owned(),
            property_bytes,
            maximum_bytes: MAX_MESSAGE_PROPERTY_BYTES,
        });
    }
    Ok(())
}

fn push_value_nodes<'a>(
    pending: &mut Vec<(&'a MessageValue, usize)>,
    values: impl Iterator<Item = &'a MessageValue>,
    depth: usize,
    items: &mut usize,
) -> Result<(), BrokerError> {
    for value in values {
        if depth > MAX_MESSAGE_VALUE_DEPTH {
            return Err(BrokerError::InvalidMessageContent {
                reason: format!("message value depth exceeds {MAX_MESSAGE_VALUE_DEPTH}"),
            });
        }
        if *items == MAX_MESSAGE_VALUE_ITEMS {
            return Err(BrokerError::InvalidMessageContent {
                reason: format!("message value count exceeds {MAX_MESSAGE_VALUE_ITEMS}"),
            });
        }
        *items += 1;
        pending.push((value, depth));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CodecError, codec};

    #[test]
    fn the_default_header_uses_the_protocol_neutral_default_priority() {
        assert_eq!(MessageHeader::default().priority, 4);
    }

    #[test]
    fn scalar_and_nested_values_round_trip_without_changing_float_bits() -> Result<(), CodecError> {
        let value = MessageValue::Map(vec![
            (
                MessageValue::String(String::from("negative-zero")),
                MessageValue::Float(0x8000_0000),
            ),
            (
                MessageValue::String(String::from("nan")),
                MessageValue::Double(0x7ff8_0000_0000_0001),
            ),
            (
                MessageValue::Uint(3),
                MessageValue::Array(vec![MessageValue::Int(4), MessageValue::Int(5)]),
            ),
            (
                MessageValue::Null,
                MessageValue::Described {
                    descriptor: MessageDescriptor::Name(String::from("urn:test")),
                    value: Box::new(MessageValue::Binary(vec![1, 2])),
                },
            ),
        ]);
        assert_eq!(
            codec::decode::<MessageValue>(&codec::encode(&value)?)?,
            value
        );
        assert_eq!(codec::encode(&value)?, codec::encode(&value)?);
        Ok(())
    }

    #[test]
    fn every_value_variant_has_deterministic_storage_encoding() -> Result<(), CodecError> {
        let values = vec![
            MessageValue::Null,
            MessageValue::Bool(false),
            MessageValue::Ubyte(u8::MAX),
            MessageValue::Ushort(u16::MAX),
            MessageValue::Uint(u32::MAX),
            MessageValue::Ulong(u64::MAX),
            MessageValue::Byte(i8::MIN),
            MessageValue::Short(i16::MIN),
            MessageValue::Int(i32::MIN),
            MessageValue::Long(i64::MIN),
            MessageValue::Float(0x7fc0_0001),
            MessageValue::Double(0x8000_0000_0000_0000),
            MessageValue::Decimal32([1; 4]),
            MessageValue::Decimal64([2; 8]),
            MessageValue::Decimal128([3; 16]),
            MessageValue::Char('\u{1f600}'),
            MessageValue::Timestamp(i64::MIN),
            MessageValue::Uuid([4; 16]),
            MessageValue::Binary(vec![0, 255]),
            MessageValue::String(String::new()),
            MessageValue::Symbol(String::from("symbol")),
            MessageValue::List(Vec::new()),
            MessageValue::Map(Vec::new()),
            MessageValue::Array(vec![MessageValue::Uint(1), MessageValue::Uint(2)]),
            MessageValue::Described {
                descriptor: MessageDescriptor::Code(123),
                value: Box::new(MessageValue::Long(-1)),
            },
        ];
        let envelope = MessageEnvelope {
            body: MessageBody::Sequence(vec![values]),
            ..MessageEnvelope::default()
        };
        assert_eq!(envelope.validate(), Ok(()));
        let encoded = codec::encode(&envelope)?;
        assert_eq!(codec::encode(&envelope)?, encoded);
        assert_eq!(codec::decode::<MessageEnvelope>(&encoded)?, envelope);
        Ok(())
    }

    #[test]
    fn missing_and_explicit_empty_identifiers_remain_distinct() -> Result<(), CodecError> {
        for message_id in [
            None,
            Some(MessageIdentifier::String(String::new())),
            Some(MessageIdentifier::Binary(Vec::new())),
            Some(MessageIdentifier::Ulong(0)),
            Some(MessageIdentifier::Uuid([0; 16])),
        ] {
            let envelope = MessageEnvelope {
                properties: MessageProperties {
                    message_id,
                    ..MessageProperties::default()
                },
                ..MessageEnvelope::default()
            };
            assert_eq!(
                codec::decode::<MessageEnvelope>(&codec::encode(&envelope)?)?,
                envelope
            );
        }
        Ok(())
    }

    fn nested_value(depth: usize, described: bool) -> MessageValue {
        (0..depth).fold(MessageValue::Null, |value, _| {
            if described {
                MessageValue::Described {
                    descriptor: MessageDescriptor::Code(123),
                    value: Box::new(value),
                }
            } else {
                MessageValue::List(vec![value])
            }
        })
    }

    #[test]
    fn root_depth_zero_allows_depth_64_but_not_65() {
        for described in [false, true] {
            let accepted = MessageEnvelope {
                body: MessageBody::Value(nested_value(MAX_MESSAGE_VALUE_DEPTH, described)),
                ..MessageEnvelope::default()
            };
            assert_eq!(accepted.validate(), Ok(()));
            let rejected = MessageEnvelope {
                body: MessageBody::Value(nested_value(MAX_MESSAGE_VALUE_DEPTH + 1, described)),
                ..MessageEnvelope::default()
            };
            assert_eq!(
                rejected.validate(),
                Err(BrokerError::InvalidMessageContent {
                    reason: String::from("message value depth exceeds 64")
                })
            );
        }
    }

    #[test]
    fn depth_is_checked_before_recursive_application_and_map_key_validation() {
        let application = MessageEnvelope {
            application_properties: BTreeMap::from([(
                String::from("deep"),
                nested_value(MAX_MESSAGE_VALUE_DEPTH + 1, true),
            )]),
            ..MessageEnvelope::default()
        };
        assert_eq!(
            application.validate(),
            Err(BrokerError::InvalidMessageContent {
                reason: String::from("message value depth exceeds 64")
            })
        );
        let map = MessageEnvelope {
            body: MessageBody::Value(MessageValue::Map(vec![(
                nested_value(MAX_MESSAGE_VALUE_DEPTH, true),
                MessageValue::Null,
            )])),
            ..MessageEnvelope::default()
        };
        assert_eq!(
            map.validate(),
            Err(BrokerError::InvalidMessageContent {
                reason: String::from("message value depth exceeds 64")
            })
        );
    }

    #[test]
    fn array_item_budget_includes_the_top_container_even_for_zero_width_values() {
        let accepted = MessageEnvelope {
            body: MessageBody::Value(MessageValue::Array(vec![
                MessageValue::Null;
                MAX_MESSAGE_VALUE_ITEMS - 1
            ])),
            ..MessageEnvelope::default()
        };
        assert_eq!(accepted.validate(), Ok(()));
        let rejected = MessageEnvelope {
            body: MessageBody::Value(MessageValue::Array(vec![
                MessageValue::Null;
                MAX_MESSAGE_VALUE_ITEMS
            ])),
            ..MessageEnvelope::default()
        };
        assert_eq!(
            rejected.validate(),
            Err(BrokerError::InvalidMessageContent {
                reason: String::from("message value count exceeds 65536")
            })
        );
    }

    #[test]
    fn value_item_budget_is_shared_across_all_envelope_sections() {
        let mut envelope = MessageEnvelope {
            application_properties: BTreeMap::from([(
                String::from("application"),
                MessageValue::Null,
            )]),
            message_annotations: BTreeMap::from([(AnnotationKey::Ulong(1), MessageValue::Null)]),
            footer: BTreeMap::from([(AnnotationKey::Ulong(2), MessageValue::Null)]),
            body: MessageBody::Sequence(vec![vec![
                MessageValue::Null;
                MAX_MESSAGE_VALUE_ITEMS - 3
            ]]),
            ..MessageEnvelope::default()
        };
        assert_eq!(envelope.validate(), Ok(()));
        envelope
            .footer
            .insert(AnnotationKey::Ulong(3), MessageValue::Null);
        assert_eq!(
            envelope.validate(),
            Err(BrokerError::InvalidMessageContent {
                reason: String::from("message value count exceeds 65536")
            })
        );
    }

    #[test]
    fn map_keys_and_values_both_consume_the_value_item_budget() {
        let envelope = MessageEnvelope {
            body: MessageBody::Value(MessageValue::Map(
                (0..MAX_MESSAGE_VALUE_ITEMS / 2)
                    .map(|index| (MessageValue::Ulong(index as u64), MessageValue::Null))
                    .collect(),
            )),
            ..MessageEnvelope::default()
        };
        assert_eq!(
            envelope.validate(),
            Err(BrokerError::InvalidMessageContent {
                reason: String::from("message value count exceeds 65536")
            })
        );
    }

    #[test]
    fn symbols_are_ascii_but_ordinary_strings_and_application_keys_may_be_utf8() {
        let invalid = MessageEnvelope {
            body: MessageBody::Value(MessageValue::Map(vec![(
                MessageValue::Symbol(String::from("\u{e9}")),
                MessageValue::Null,
            )])),
            ..MessageEnvelope::default()
        };
        assert!(matches!(
            invalid.validate(),
            Err(BrokerError::InvalidMessageContent { .. })
        ));
        let valid = MessageEnvelope {
            application_properties: BTreeMap::from([(
                String::from("\u{e9}"),
                MessageValue::String(String::from("\u{e9}")),
            )]),
            body: MessageBody::Value(MessageValue::String(String::from("\u{1f600}"))),
            ..MessageEnvelope::default()
        };
        assert_eq!(valid.validate(), Ok(()));
    }

    #[test]
    fn application_property_updates_bound_the_combined_header_before_copying() {
        let accepted = BTreeMap::from([
            (String::from("first"), MessageValue::Binary(vec![0; 21_000])),
            (
                String::from("second"),
                MessageValue::Binary(vec![0; 21_000]),
            ),
            (String::from("third"), MessageValue::Binary(vec![0; 21_000])),
        ]);
        assert_eq!(
            MessageEnvelope::validate_application_property_updates(&accepted),
            Ok(())
        );
        let rejected = BTreeMap::from([
            (String::from("first"), MessageValue::Binary(vec![0; 22_000])),
            (
                String::from("second"),
                MessageValue::Binary(vec![0; 22_000]),
            ),
            (String::from("third"), MessageValue::Binary(vec![0; 22_000])),
        ]);
        assert!(matches!(
            MessageEnvelope::validate_application_property_updates(&rejected),
            Err(BrokerError::MessageHeaderTooLarge {
                maximum_bytes: MAX_MESSAGE_HEADER_BYTES,
                ..
            })
        ));
    }

    #[test]
    fn application_property_updates_bound_described_chains_before_copying() {
        let described = |depth| {
            let mut value = MessageValue::Null;
            for _ in 0..depth {
                value = MessageValue::Described {
                    descriptor: MessageDescriptor::Code(1),
                    value: Box::new(value),
                };
            }
            BTreeMap::from([(String::from("key"), value)])
        };
        assert_eq!(
            MessageEnvelope::validate_application_property_updates(&described(
                MAX_MESSAGE_VALUE_DEPTH
            )),
            Ok(())
        );
        assert!(matches!(
            MessageEnvelope::validate_application_property_updates(&described(
                MAX_MESSAGE_VALUE_DEPTH + 1
            )),
            Err(BrokerError::InvalidMessageContent { .. })
        ));
    }

    #[test]
    fn content_tally_includes_nested_values_and_metadata() {
        let mut envelope = MessageEnvelope::default();
        let initial = envelope.content_size();
        envelope.message_annotations.insert(
            AnnotationKey::Symbol(String::from("key")),
            MessageValue::List(vec![MessageValue::String("x".repeat(100))]),
        );
        assert!(envelope.content_size() >= initial + 103);
        envelope.body = MessageBody::Data(vec![vec![0; 200], vec![0; 300]]);
        assert!(envelope.content_size() >= initial + 603);
        envelope
            .footer
            .insert(AnnotationKey::Ulong(7), MessageValue::Binary(vec![0; 50]));
        assert!(envelope.content_size() >= initial + 653);
    }
}
