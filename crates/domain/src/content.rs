//! Normalized send fields derived from original message content.
//!
//! The derivation preserves legacy duplicate identities and selects legacy or
//! Azure SDK text for filter IDs explicitly from the content variant. It derives
//! the session from `group-id`,
//! the header TTL, the scheduled-enqueue annotation, and the filter projection.
//! Filterable application-property values use the canonical AMQP encoding that
//! correlation rule values are compared with. Legacy intent keeps its original
//! rule-matching behavior during replay.

use std::collections::{BTreeMap, HashMap};

use message_content::{
    AnnotationKey, Constructor, Content, Descriptor, Field, Limits, MessageId,
    PrimitiveConstructor, Value, decode_content, decode_value,
};
use thiserror::Error;

use crate::{
    CorrelationValue, FilterProperties, MAX_CORRELATION_VALUE_BYTES, MessageContent,
    MessageEnvelope, MessageInput, OverlayProperty, SessionId, Timestamp,
};

/// The fixed V3 content-safety profile every parse of original content uses.
/// It bounds parser work only; queue size limits and the proposal envelope
/// limit are separate budgets.
pub const CONTENT_LIMITS: Limits = Limits {
    max_bytes: 16 * 1024 * 1024,
    max_depth: 128,
    max_nodes: 4_194_304,
    max_work: 268_435_456,
};

const SCHEDULED_ENQUEUE_TIME_ANNOTATION: &str = "x-opt-scheduled-enqueue-time";

/// Why original content cannot become a message. Decided from the bytes alone.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum MessageContentError {
    #[error("AMQP message content is invalid: {0}")]
    Amqp(message_content::Error),
    #[error("the AMQP group-id is not a valid session identifier")]
    InvalidSessionId,
    #[error("x-opt-scheduled-enqueue-time must be an AMQP timestamp")]
    ScheduledEnqueueTimeNotTimestamp,
    #[error("x-opt-scheduled-enqueue-time cannot precede the Unix epoch")]
    ScheduledEnqueueTimeBeforeEpoch,
    #[error("a filterable application property exceeds {maximum} encoded bytes")]
    PropertyValueTooLarge { maximum: usize },
    #[error("the derived filter projection exceeds its total size budget")]
    ProjectionTooLarge,
    #[error("a derived filter-projection value is not decodable bounded AMQP")]
    ProjectionValueUndecodable,
    #[error("an AMQP array of compound, described or non-ASCII-text elements is not sendable")]
    UnsupportedArrayElement,
    #[error("a property patch value is not one bounded AMQP value: {0}")]
    PatchValueInvalid(message_content::Error),
    #[error("a property patch needs a copy with AMQP content to apply to")]
    PatchWithoutAmqpContent,
    #[error("a replacement envelope would drop the copy's property overlay")]
    ReplacementWouldDropOverlay,
}

/// Fixed allowance a patched copy may exceed its entity's message-size limit by.
///
/// Service Bus caps a message's header (system and application properties) at
/// 64 KiB on every tier, so a properties-to-modify patch on a message admitted
/// exactly at the limit, including the one a dead-letter disposition carries,
/// always fits. Being fixed, it bounds cumulative overlay growth per copy at the
/// entity limit plus 64 KiB, and never past the content-safety profile's
/// `max_bytes`, which every rendered copy must fit. Each patch arrives in its
/// own proposal, so the one-MiB proposal envelope bounds a single patch, not
/// the accumulated overlay.
pub const PROPERTY_PATCH_HEADROOM_BYTES: usize = 64 * 1024;

/// Applies a sparse patch to a copy's overlay. Each value must be one bounded
/// AMQP value; an explicit null (`0x40`) is kept as a value, never a deletion.
/// A name already present keeps its position and takes the new value, so the
/// result stays unique by name in first-insertion order.
pub(crate) fn patch_overlay(
    overlay: &[OverlayProperty],
    patch: &[OverlayProperty],
) -> Result<Vec<OverlayProperty>, MessageContentError> {
    let mut merged = overlay.to_vec();
    let mut positions: HashMap<&str, usize> = overlay
        .iter()
        .enumerate()
        .map(|(index, property)| (property.name.as_str(), index))
        .collect();
    for property in patch {
        decode_value(&property.value, CONTENT_LIMITS)
            .map_err(MessageContentError::PatchValueInvalid)?;
        match positions.get(property.name.as_str()) {
            Some(&index) => merged[index].value.clone_from(&property.value),
            None => {
                positions.insert(&property.name, merged.len());
                merged.push(property.clone());
            }
        }
    }
    Ok(merged)
}

/// Bytes a copy's effective message is charged against its entity limit: the
/// original plus every overlay name and value. Per-entry AMQP framing of a
/// rendered message is not counted; the fixed headroom absorbs it.
pub(crate) fn effective_len(original: &[u8], overlay: &[OverlayProperty]) -> usize {
    overlay.iter().fold(original.len(), |total, property| {
        total
            .saturating_add(property.name.len())
            .saturating_add(property.value.len())
    })
}

/// The filter projection a send of the effective message would derive: the
/// original's projection with the overlay applied over its application
/// properties. It uses the send derivation's encoder and its budget formula,
/// charging the effective length rather than the original alone. An original
/// that is not bounded AMQP refuses, so a legacy V1 envelope that cannot be
/// parsed is never patched.
pub(crate) fn effective_projection(
    original: &[u8],
    overlay: &[OverlayProperty],
    text_ids: bool,
) -> Result<FilterProperties, MessageContentError> {
    let content = decode_content(original, CONTENT_LIMITS).map_err(MessageContentError::Amqp)?;
    let values = overlay
        .iter()
        .map(|property| {
            decode_value(&property.value, CONTENT_LIMITS)
                .map_err(MessageContentError::PatchValueInvalid)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut budget = effective_len(original, overlay).saturating_add(MAX_CORRELATION_VALUE_BYTES);
    project(
        &content,
        overlay
            .iter()
            .zip(&values)
            .map(|(property, value)| (property.name.as_str(), value))
            .collect(),
        &mut budget,
        text_ids,
    )
}

/// Why canonical correlation encoding stopped. Mapped to a [`MessageContentError`].
enum EncodeError {
    /// The encoding grew past the retained correlation-value ceiling.
    TooLarge,
    /// The value nests an array this encoder cannot store as spec AMQP: compound
    /// or described elements, an empty-list (`list0`) element, or a non-ASCII
    /// string or symbol element. See `scalar_array_constructor` and the string
    /// and symbol arms of `encode`.
    UnsupportedArray,
}

impl MessageContent {
    /// The send input a single content send stands for. AMQP content keeps its
    /// original bytes as the envelope and an empty body, because a V2/V3 record
    /// stores the content once; raw content keeps its bytes as the body.
    pub fn derive(&self) -> Result<MessageInput, MessageContentError> {
        let mut budget = self
            .original_len()
            .saturating_add(MAX_CORRELATION_VALUE_BYTES);
        self.derive_within(&mut budget)
    }

    /// Derives every child of one batch command against a single projection
    /// budget, so a batch cannot multiply the per-command allowance by its child
    /// count. All children derive before any state is read, so an invalid or
    /// over-budget child refuses the whole command with no effects.
    pub fn derive_batch(messages: &[Self]) -> Result<Vec<MessageInput>, MessageContentError> {
        // One command-wide budget: the children's combined original length plus
        // a single correlation-value ceiling of slack. Original content is
        // bounded by the one-MiB proposal envelope, so this stays linear.
        let mut budget = messages
            .iter()
            .fold(MAX_CORRELATION_VALUE_BYTES, |total, message| {
                total.saturating_add(message.original_len())
            });
        messages
            .iter()
            .map(|message| message.derive_within(&mut budget))
            .collect()
    }

    /// Original byte length this content contributes to the per-command budget.
    fn original_len(&self) -> usize {
        match self {
            Self::Amqp(bytes) | Self::AmqpTextIds(bytes) => bytes.len(),
            Self::Raw(raw) => raw.body.len(),
        }
    }

    fn derive_within(&self, remaining: &mut usize) -> Result<MessageInput, MessageContentError> {
        match self {
            // Raw content carries no AMQP projection, so it spends no budget.
            Self::Raw(raw) => Ok(MessageInput {
                message_id: raw.message_id.clone(),
                body: raw.body.clone(),
                time_to_live_millis: raw.time_to_live_millis,
                session_id: raw.session_id.clone(),
                scheduled_enqueue_at: raw.scheduled_enqueue_at,
                envelope: None,
            }),
            Self::Amqp(bytes) => derive_amqp(bytes, remaining, false),
            Self::AmqpTextIds(bytes) => derive_amqp(bytes, remaining, true),
        }
    }
}

fn derive_amqp(
    bytes: &[u8],
    remaining_total: &mut usize,
    text_ids: bool,
) -> Result<MessageInput, MessageContentError> {
    let content = decode_content(bytes, CONTENT_LIMITS).map_err(MessageContentError::Amqp)?;
    let properties = content.properties.as_ref();
    let session_id = properties
        .and_then(|properties| value(&properties.group_id))
        .map(|group| SessionId::new(group.text.as_str()))
        .transpose()
        .map_err(|_| MessageContentError::InvalidSessionId)?;
    let scheduled_enqueue_at = scheduled_enqueue_at(content.message_annotations.as_deref())?;
    let projection = project(&content, Vec::new(), remaining_total, text_ids)?;

    Ok(MessageInput {
        // This is the durable duplicate-detection identity, not filter text.
        // Changing it would invalidate retained history and cross-type aliases.
        message_id: properties
            .and_then(|properties| value(&properties.message_id))
            .map(message_id_text)
            .unwrap_or_default(),
        body: Vec::new(),
        time_to_live_millis: content
            .header
            .as_ref()
            .and_then(|header| value(&header.ttl))
            .map(|ttl| u64::from(*ttl)),
        session_id,
        scheduled_enqueue_at,
        envelope: Some(MessageEnvelope::new(bytes.to_vec()).with_filter_properties(projection)),
    })
}

/// The filter projection of decoded content whose application properties are
/// followed by `overlay`, each overlay value replacing or adding the property of
/// its name. A send passes no overlay; a copy-local patch passes its effective
/// overlay, so the result is what a send of the effective message derives.
fn project<'a>(
    content: &'a Content,
    overlay: Vec<(&'a str, &'a Value)>,
    remaining_total: &mut usize,
    text_ids: bool,
) -> Result<FilterProperties, MessageContentError> {
    let properties = content.properties.as_ref();
    // Ingress collapses duplicate application properties to the last value
    // before encoding any of them.
    let mut application = BTreeMap::new();
    for (name, value) in content.application_properties.iter().flatten() {
        application.insert(name.text.as_str(), value);
    }
    for (name, value) in overlay {
        application.insert(name, value);
    }
    // The shared per-command budget spans every projected value of every child.
    // Each value is also capped at MAX_CORRELATION_VALUE_BYTES individually;
    // the command budget, linear in the children's combined original content,
    // stops many separately-small values (for example zero-width array elements
    // that each re-encode wider), and whole batches of them, from summing into a
    // projection far larger than the input, held in one WriteBatch and copied
    // once per subscription.
    let application_properties = application
        .into_iter()
        .map(|(name, value)| {
            // The per-value budget keeps every value at or below the ceiling, so
            // CorrelationValue::new can only fail if the ceiling itself ever
            // dropped below the length check, which it does not.
            let bytes = encode_correlation_value(value)?;
            *remaining_total = remaining_total
                .checked_sub(bytes.len())
                .ok_or(MessageContentError::ProjectionTooLarge)?;
            // Safety net: the stored projection must round-trip through the same
            // bounded value decoder that snapshot capture, compaction and store
            // upgrade check it with, so a divergent encoding is refused here
            // rather than wedging those offline paths later.
            decode_value(&bytes, CONTENT_LIMITS)
                .map_err(|_| MessageContentError::ProjectionValueUndecodable)?;
            CorrelationValue::new(bytes)
                .map(|value| (name.to_owned(), value))
                .map_err(|_| MessageContentError::PropertyValueTooLarge {
                    maximum: MAX_CORRELATION_VALUE_BYTES,
                })
        })
        .collect::<Result<_, _>>()?;
    let text = |field: Option<&Field<message_content::Text>>| {
        field.and_then(value).map(|text| text.text.clone())
    };
    let id = |id: &MessageId| {
        if text_ids {
            azure_message_id_text(id)
        } else {
            message_id_text(id)
        }
    };
    Ok(FilterProperties {
        correlation_id: properties
            .and_then(|properties| value(&properties.correlation_id))
            .map(id),
        message_id: properties
            .and_then(|properties| value(&properties.message_id))
            .map(id),
        to: text(properties.map(|properties| &properties.to)),
        reply_to: text(properties.map(|properties| &properties.reply_to)),
        subject: text(properties.map(|properties| &properties.subject)),
        session_id: text(properties.map(|properties| &properties.group_id)),
        reply_to_session_id: text(properties.map(|properties| &properties.reply_to_group_id)),
        content_type: properties
            .and_then(|properties| value(&properties.content_type))
            .map(|symbol| symbol.text.clone()),
        application_properties,
    })
}

fn value<T>(field: &Field<T>) -> Option<&T> {
    match field {
        Field::Value(value) => Some(value),
        Field::Absent | Field::Null => None,
    }
}

fn scheduled_enqueue_at(
    annotations: Option<&[(AnnotationKey, Value)]>,
) -> Result<Option<Timestamp>, MessageContentError> {
    // A repeated annotation key keeps its last value, as ingress does.
    let Some((_, value)) = annotations.into_iter().flatten().rev().find(|(key, _)| {
        matches!(key, AnnotationKey::Symbol(symbol)
            if symbol.text == SCHEDULED_ENQUEUE_TIME_ANNOTATION)
    }) else {
        return Ok(None);
    };
    let Value::Timestamp(millis) = value else {
        return Err(MessageContentError::ScheduledEnqueueTimeNotTimestamp);
    };
    u64::try_from(*millis)
        .map(|millis| Some(Timestamp::from_millis(millis)))
        .map_err(|_| MessageContentError::ScheduledEnqueueTimeBeforeEpoch)
}

fn message_id_text(message_id: &MessageId) -> String {
    match message_id {
        MessageId::String(text) => text.text.clone(),
        MessageId::ULong(value) => value.to_string(),
        // The textual form ingress has always produced for a UUID identifier.
        MessageId::Uuid(bytes) => format!("Uuid({bytes:?})"),
        MessageId::Binary(binary) => binary
            .bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    }
}

/// Azure AMQP SDK MessageId.ToString(): GUID D format, invariant decimal,
/// uppercase hexadecimal binary without separators, and unchanged strings.
/// This is filter text only; the original typed AMQP identifier is retained.
fn azure_message_id_text(message_id: &MessageId) -> String {
    match message_id {
        MessageId::String(text) => text.text.clone(),
        MessageId::ULong(value) => value.to_string(),
        MessageId::Uuid(bytes) => {
            let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            format!(
                "{}-{}-{}-{}-{}",
                &hex[..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..]
            )
        }
        MessageId::Binary(binary) => binary
            .bytes
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect(),
    }
}

/// Where a value sits relative to an enclosing array, which decides whether it
/// writes its own constructor and which widths it uses.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Position {
    Standalone,
    FirstElement,
    OtherElement,
}

/// The canonical correlation encoding of one application-property value: the
/// bytes the AMQP library's serializer writes for the decoded value, which is
/// what rule values are compared with. The output is bounded by the retained
/// correlation-value ceiling so a single huge or deeply duplicated value cannot
/// cost unbounded work on every apply and replay.
fn encode_correlation_value(value: &Value) -> Result<Vec<u8>, MessageContentError> {
    let mut out = Vec::new();
    // A value may reach, but not exceed, the retained correlation-value ceiling,
    // so the budget is exactly that ceiling. Nested containers are buffered and
    // concatenated upward rather than back-patched in place, because an AMQP
    // compound's size/count width is only known after its body and would shift
    // the header; depth is bounded to 128 by the content profile and the output
    // to the ceiling, so that copying stays bounded.
    let mut remaining = MAX_CORRELATION_VALUE_BYTES;
    encode(value, Position::Standalone, &mut out, &mut remaining).map_err(|error| match error {
        EncodeError::TooLarge => MessageContentError::PropertyValueTooLarge {
            maximum: MAX_CORRELATION_VALUE_BYTES,
        },
        EncodeError::UnsupportedArray => MessageContentError::UnsupportedArrayElement,
    })?;
    Ok(out)
}

/// Appends produced bytes, refusing once the running output passes the ceiling.
/// Only bytes that enter the result are charged; a parent concatenating an
/// already-charged child buffer uses [`Vec::extend_from_slice`] directly.
fn put(out: &mut Vec<u8>, remaining: &mut usize, bytes: &[u8]) -> Result<(), EncodeError> {
    *remaining = remaining
        .checked_sub(bytes.len())
        .ok_or(EncodeError::TooLarge)?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn encode(
    value: &Value,
    position: Position,
    out: &mut Vec<u8>,
    remaining: &mut usize,
) -> Result<(), EncodeError> {
    let standalone = position == Position::Standalone;
    match value {
        Value::Null => put(out, remaining, &[0x40]),
        Value::Bool(value) => match position {
            Position::Standalone => put(out, remaining, &[if *value { 0x41 } else { 0x42 }]),
            Position::FirstElement => put(out, remaining, &[0x56, u8::from(*value)]),
            Position::OtherElement => put(out, remaining, &[u8::from(*value)]),
        },
        Value::UByte(value) => fixed(out, remaining, position, 0x50, &[*value]),
        Value::UShort(value) => fixed(out, remaining, position, 0x60, &value.to_be_bytes()),
        Value::UInt(0) if standalone => put(out, remaining, &[0x43]),
        Value::UInt(value @ 1..=255) if standalone => put(out, remaining, &[0x52, *value as u8]),
        Value::UInt(value) => fixed(out, remaining, position, 0x70, &value.to_be_bytes()),
        Value::ULong(0) if standalone => put(out, remaining, &[0x44]),
        Value::ULong(value @ 1..=255) if standalone => put(out, remaining, &[0x53, *value as u8]),
        Value::ULong(value) => fixed(out, remaining, position, 0x80, &value.to_be_bytes()),
        Value::Byte(value) => fixed(out, remaining, position, 0x51, &value.to_be_bytes()),
        Value::Short(value) => fixed(out, remaining, position, 0x61, &value.to_be_bytes()),
        Value::Int(value) if standalone && i8::try_from(*value).is_ok() => {
            put(out, remaining, &[0x54, *value as u8])
        }
        Value::Int(value) => fixed(out, remaining, position, 0x71, &value.to_be_bytes()),
        Value::Long(value) if standalone && i8::try_from(*value).is_ok() => {
            put(out, remaining, &[0x55, *value as u8])
        }
        Value::Long(value) => fixed(out, remaining, position, 0x81, &value.to_be_bytes()),
        Value::Float(bits) => fixed(out, remaining, position, 0x72, &bits.to_be_bytes()),
        Value::Double(bits) => fixed(out, remaining, position, 0x82, &bits.to_be_bytes()),
        Value::Decimal32(bits) => fixed(out, remaining, position, 0x74, &bits.to_be_bytes()),
        Value::Decimal64(bits) => fixed(out, remaining, position, 0x84, &bits.to_be_bytes()),
        Value::Decimal128(bytes) => fixed(out, remaining, position, 0x94, bytes),
        Value::Char(value) => fixed(
            out,
            remaining,
            position,
            0x73,
            &u32::from(*value).to_be_bytes(),
        ),
        Value::Timestamp(millis) => fixed(out, remaining, position, 0x83, &millis.to_be_bytes()),
        Value::Uuid(bytes) => fixed(out, remaining, position, 0x98, bytes),
        Value::Binary(binary) => {
            variable(position, 0xa0, 0xb0, binary.bytes.len(), out, remaining)?;
            put(out, remaining, &binary.bytes)
        }
        Value::String(text) => {
            // A standalone string prefixes its byte length. An array element
            // prefixes its length in characters, which only equals the byte
            // length for ASCII; a non-ASCII element would store a byte count
            // that disagrees with the text and is not decodable AMQP, so such an
            // array is refused rather than stored.
            if !standalone && !text.text.is_ascii() {
                return Err(EncodeError::UnsupportedArray);
            }
            variable(position, 0xa1, 0xb1, text.text.len(), out, remaining)?;
            put(out, remaining, text.text.as_bytes())
        }
        Value::Symbol(symbol) => {
            // Symbols are ASCII by definition; a non-ASCII array element cannot
            // be rendered as a spec symbol, so the array is refused.
            if !standalone && !symbol.text.is_ascii() {
                return Err(EncodeError::UnsupportedArray);
            }
            variable(position, 0xa3, 0xb3, symbol.text.len(), out, remaining)?;
            put(out, remaining, symbol.text.as_bytes())
        }
        Value::List(values) => {
            let mut items = Vec::new();
            for value in values {
                encode(value, Position::Standalone, &mut items, remaining)?;
            }
            if items.is_empty() {
                put(out, remaining, &[0x45])
            } else {
                compound(position, 0xc0, 0xd0, values.len(), &items, out, remaining)
            }
        }
        Value::Map(pairs) => {
            // Decoding keeps a repeated key at its first position with its
            // last value, exactly as the AMQP library's ordered map does.
            // Deduplicating through a hash of the encoded key keeps this linear
            // rather than scanning every prior entry.
            let mut positions: HashMap<Vec<u8>, usize> = HashMap::new();
            let mut entries: Vec<(Vec<u8>, &Value)> = Vec::new();
            for (key, value) in pairs {
                // Each key is encoded against its own ceiling so a large but
                // repeated key is not wrongly refused for exhausting the shared
                // budget before the repeat is noticed. The shared budget is
                // charged only once per kept key, since only kept keys and
                // values reach the result.
                let mut encoded = Vec::new();
                let mut key_budget = MAX_CORRELATION_VALUE_BYTES;
                encode(key, Position::Standalone, &mut encoded, &mut key_budget)?;
                match positions.get(&encoded) {
                    Some(&index) => entries[index].1 = value,
                    None => {
                        *remaining = remaining
                            .checked_sub(encoded.len())
                            .ok_or(EncodeError::TooLarge)?;
                        positions.insert(encoded.clone(), entries.len());
                        entries.push((encoded, value));
                    }
                }
            }
            let mut items = Vec::new();
            for (key, value) in &entries {
                // The key was charged while it was first encoded above.
                items.extend_from_slice(key);
                encode(value, Position::Standalone, &mut items, remaining)?;
            }
            compound(
                position,
                0xc1,
                0xd1,
                entries.len() * 2,
                &items,
                out,
                remaining,
            )
        }
        Value::Array(array) => {
            // Only arrays of plain scalar elements are sendable. Arrays of
            // compound or described elements are refused (AMQP ingress refuses
            // those too); arrays of empty lists are refused because this encoder
            // has no spec rendering for them. See `scalar_array_constructor`.
            if !scalar_array_constructor(&array.constructor) {
                return Err(EncodeError::UnsupportedArray);
            }
            let mut items = Vec::new();
            for (index, element) in array.elements.iter().enumerate() {
                let element_position = if index == 0 {
                    Position::FirstElement
                } else {
                    Position::OtherElement
                };
                encode(element, element_position, &mut items, remaining)?;
            }
            compound(
                position,
                0xe0,
                0xf0,
                array.elements.len(),
                &items,
                out,
                remaining,
            )
        }
        Value::Described { descriptor, value } => {
            put(out, remaining, &[0x00])?;
            match descriptor {
                Descriptor::ULong(code) => encode(&Value::ULong(*code), position, out, remaining)?,
                Descriptor::Symbol(symbol) => {
                    encode(&Value::Symbol(symbol.clone()), position, out, remaining)?
                }
            }
            encode(value, position, out, remaining)
        }
    }
}

fn fixed(
    out: &mut Vec<u8>,
    remaining: &mut usize,
    position: Position,
    code: u8,
    payload: &[u8],
) -> Result<(), EncodeError> {
    if position != Position::OtherElement {
        put(out, remaining, &[code])?;
    }
    put(out, remaining, payload)
}

/// Whether an array's shared element constructor is an accepted scalar.
///
/// Compound (`list8/32`, `map8/32`, `array8/32`) and described constructors are
/// refused, as AMQP ingress refuses them too. The empty-list constructor
/// `list0` is also refused, but for a different reason: ingress accepts and
/// re-encodes a `list0` array, whereas this encoder would emit one `0x45` per
/// element (for `[0xe0, 2, 2, 0x45]`, the bytes `e0 03 02 45 45`), which
/// `decode_value` rejects, so it is refused rather than stored.
fn scalar_array_constructor(constructor: &Constructor) -> bool {
    match constructor {
        // Empty arrays carry no element constructor.
        Constructor::Absent | Constructor::Null => true,
        Constructor::Described { .. } => false,
        Constructor::Primitive(code) => !matches!(
            code,
            PrimitiveConstructor::List0
                | PrimitiveConstructor::List8
                | PrimitiveConstructor::List32
                | PrimitiveConstructor::Map8
                | PrimitiveConstructor::Map32
                | PrimitiveConstructor::Array8
                | PrimitiveConstructor::Array32
        ),
    }
}

fn variable(
    position: Position,
    short: u8,
    long: u8,
    length: usize,
    out: &mut Vec<u8>,
    remaining: &mut usize,
) -> Result<(), EncodeError> {
    match position {
        Position::Standalone if length < 255 => put(out, remaining, &[short, length as u8]),
        Position::Standalone | Position::FirstElement => {
            put(out, remaining, &[long])?;
            put(out, remaining, &(length as u32).to_be_bytes())
        }
        Position::OtherElement => put(out, remaining, &(length as u32).to_be_bytes()),
    }
}

/// Lists, maps and arrays share one framing rule: a one-byte size and count
/// while the payload stays below 255 bytes, otherwise four-byte fields. The
/// `items` body is already charged against the budget, so only the header is.
fn compound(
    position: Position,
    short: u8,
    long: u8,
    count: usize,
    items: &[u8],
    out: &mut Vec<u8>,
    remaining: &mut usize,
) -> Result<(), EncodeError> {
    if items.len() < 255 {
        if position != Position::OtherElement {
            put(out, remaining, &[short])?;
        }
        put(out, remaining, &[(items.len() + 1) as u8, count as u8])?;
    } else {
        if position != Position::OtherElement {
            put(out, remaining, &[long])?;
        }
        put(out, remaining, &((items.len() + 4) as u32).to_be_bytes())?;
        put(out, remaining, &(count as u32).to_be_bytes())?;
    }
    out.extend_from_slice(items);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use message_content::{Array, Binary, Symbol, Text, Width};

    fn string(value: &str) -> Value {
        Value::String(Text {
            text: value.into(),
            width: Width::One,
        })
    }

    #[test]
    fn a_map_keeps_a_repeated_key_at_its_first_position_with_its_last_value() {
        let map = Value::Map(vec![
            (string("a"), Value::Int(1)),
            (string("b"), Value::Int(2)),
            (string("a"), Value::Int(3)),
        ]);
        // map8 size 11 count 4 | a1 01 'a' 54 03 | a1 01 'b' 54 02
        assert_eq!(
            encode_correlation_value(&map).unwrap(),
            vec![0xc1, 11, 4, 0xa1, 1, b'a', 0x54, 3, 0xa1, 1, b'b', 0x54, 2]
        );
    }

    #[test]
    fn a_duplicate_heavy_map_collapses_in_bounded_work() {
        // The former linear-scan dedup was quadratic in the pair count; this
        // input would have cost ~4e10 comparisons. One entry survives.
        let pairs = (0..200_000)
            .map(|index| (string("k"), Value::Int(index % 100)))
            .collect();
        assert_eq!(
            encode_correlation_value(&Value::Map(pairs)).unwrap(),
            vec![0xc1, 6, 2, 0xa1, 1, b'k', 0x54, 99]
        );
    }

    #[test]
    fn encoding_stops_once_the_output_passes_the_ceiling() {
        let exact = Value::Binary(Binary {
            bytes: vec![0; MAX_CORRELATION_VALUE_BYTES - 5],
            width: Width::Four,
        });
        assert!(encode_correlation_value(&exact).is_ok());
        let over = Value::Binary(Binary {
            bytes: vec![0; MAX_CORRELATION_VALUE_BYTES + 1],
            width: Width::Four,
        });
        assert_eq!(
            encode_correlation_value(&over),
            Err(MessageContentError::PropertyValueTooLarge {
                maximum: MAX_CORRELATION_VALUE_BYTES
            })
        );
        // A map of distinct keys whose body exceeds the ceiling refuses without
        // materializing the whole encoding.
        let pairs = (0..MAX_CORRELATION_VALUE_BYTES)
            .map(|index| (Value::ULong(index as u64), Value::ULong(index as u64)))
            .collect();
        assert_eq!(
            encode_correlation_value(&Value::Map(pairs)),
            Err(MessageContentError::PropertyValueTooLarge {
                maximum: MAX_CORRELATION_VALUE_BYTES
            })
        );
    }

    #[test]
    fn arrays_of_compound_or_described_elements_refuse_but_scalars_encode() {
        let array = |constructor, elements| {
            Value::Array(Array {
                constructor,
                elements,
            })
        };
        for refused in [
            array(
                Constructor::Primitive(PrimitiveConstructor::List32),
                vec![Value::List(vec![Value::Int(1)])],
            ),
            array(
                Constructor::Primitive(PrimitiveConstructor::Map8),
                vec![Value::Map(vec![(Value::Int(1), Value::Int(2))])],
            ),
            // An empty-list element array has no spec rendering here.
            array(
                Constructor::Primitive(PrimitiveConstructor::List0),
                vec![Value::List(vec![]), Value::List(vec![])],
            ),
            array(
                Constructor::Described {
                    descriptor: Descriptor::ULong(1),
                    constructor: Box::new(Constructor::Primitive(PrimitiveConstructor::SmallInt)),
                },
                vec![Value::Int(1)],
            ),
        ] {
            assert_eq!(
                encode_correlation_value(&refused),
                Err(MessageContentError::UnsupportedArrayElement)
            );
        }
        let scalars = array(
            Constructor::Primitive(PrimitiveConstructor::SmallInt),
            vec![Value::Int(1), Value::Int(-1)],
        );
        assert!(encode_correlation_value(&scalars).is_ok());
        let symbol = Value::Symbol(Symbol {
            text: "s".into(),
            width: Width::One,
        });
        assert!(encode_correlation_value(&symbol).is_ok());
    }

    #[test]
    fn an_ascii_string_array_encodes_but_a_non_ascii_element_refuses() {
        let array = |elements| {
            Value::Array(Array {
                constructor: Constructor::Primitive(PrimitiveConstructor::String8),
                elements,
            })
        };
        assert!(encode_correlation_value(&array(vec![string("ab"), string("c")])).is_ok());
        assert_eq!(
            encode_correlation_value(&array(vec![string("é"), string("x")])),
            Err(MessageContentError::UnsupportedArrayElement)
        );
    }

    #[test]
    fn a_large_repeated_key_is_encoded_against_its_own_bound_not_the_shared_one() {
        // A 130 KiB key repeated twice collapses to a single entry well under
        // the ceiling. Charging each key against the shared budget would have
        // refused the second, transient copy before noticing the repeat.
        let key = Value::String(Text {
            text: "k".repeat(130 * 1024),
            width: Width::Four,
        });
        let map = Value::Map(vec![(key.clone(), Value::Null), (key, Value::Null)]);
        let bytes = encode_correlation_value(&map).expect("the repeated key is accepted");
        assert!(bytes.len() < MAX_CORRELATION_VALUE_BYTES);
        decode_value(&bytes, CONTENT_LIMITS).expect("the result is decodable AMQP");
    }

    /// An application-properties section whose values are the given compact
    /// arrays: each is a `uint0` element array that re-encodes four bytes wide.
    fn zero_width_array_message(names: &[&str], count: u32) -> Vec<u8> {
        let mut array = vec![0xf0, 0, 0, 0, 5];
        array.extend(count.to_be_bytes());
        array.push(0x43);
        let mut map = Vec::new();
        for name in names {
            map.push(0xa1);
            map.push(name.len() as u8);
            map.extend(name.as_bytes());
            map.extend(&array);
        }
        let mut section = vec![0x00, 0x53, 0x74];
        section.extend([0xc1, (map.len() + 1) as u8, (names.len() * 2) as u8]);
        section.extend(map);
        section
    }

    #[test]
    fn the_total_projection_budget_refuses_zero_width_array_amplification() {
        // Each array alone is under the per-value ceiling, but two of them
        // re-encode past the total budget, which is linear in the tiny input.
        let one = zero_width_array_message(&["a"], 60_000);
        assert!(MessageContent::Amqp(one).derive().is_ok());
        let two = zero_width_array_message(&["a", "b"], 60_000);
        assert_eq!(
            MessageContent::Amqp(two).derive(),
            Err(MessageContentError::ProjectionTooLarge)
        );
    }
}
