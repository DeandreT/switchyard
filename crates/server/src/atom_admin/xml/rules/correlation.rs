use std::{borrow::Cow, collections::BTreeSet};

use chrono::{DateTime, Utc};
use domain::{CorrelationFilter, MessageValue};

use super::super::lexical;
use super::{RuleXmlError, ordinal::KeyCasing};

pub(super) const XSD_NS: &str = "http://www.w3.org/2001/XMLSchema";
const MIN_TIMESTAMP_MILLIS: i64 = -62_135_596_800_000;
const MAX_TIMESTAMP_MILLIS: i64 = 253_402_300_799_999;
pub(super) const PROPERTY_MARKUP_BYTES: usize = 256;

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum Field {
    CorrelationId,
    MessageId,
    To,
    ReplyTo,
    Subject,
    SessionId,
    ReplyToSessionId,
    ContentType,
}

impl Field {
    pub(super) fn named(name: &str) -> Option<Self> {
        match name {
            "CorrelationId" => Some(Self::CorrelationId),
            "MessageId" => Some(Self::MessageId),
            "To" => Some(Self::To),
            "ReplyTo" => Some(Self::ReplyTo),
            "Label" => Some(Self::Subject),
            "SessionId" => Some(Self::SessionId),
            "ReplyToSessionId" => Some(Self::ReplyToSessionId),
            "ContentType" => Some(Self::ContentType),
            _ => None,
        }
    }

    pub(super) fn set(self, filter: &mut CorrelationFilter, value: String) {
        let target = match self {
            Self::CorrelationId => &mut filter.correlation_id,
            Self::MessageId => &mut filter.message_id,
            Self::To => &mut filter.to,
            Self::ReplyTo => &mut filter.reply_to,
            Self::Subject => &mut filter.subject,
            Self::SessionId => &mut filter.session_id,
            Self::ReplyToSessionId => &mut filter.reply_to_session_id,
            Self::ContentType => &mut filter.content_type,
        };
        *target = Some(value);
    }
}

pub(super) fn fields(filter: &CorrelationFilter) -> [(&'static str, Option<&str>); 8] {
    [
        ("CorrelationId", filter.correlation_id.as_deref()),
        ("MessageId", filter.message_id.as_deref()),
        ("To", filter.to.as_deref()),
        ("ReplyTo", filter.reply_to.as_deref()),
        ("Label", filter.subject.as_deref()),
        ("SessionId", filter.session_id.as_deref()),
        ("ReplyToSessionId", filter.reply_to_session_id.as_deref()),
        ("ContentType", filter.content_type.as_deref()),
    ]
}

#[derive(Clone, Copy)]
pub(super) enum ScalarKind {
    String,
    Int,
    Long,
    Bool,
    Double,
    Timestamp,
}

impl ScalarKind {
    pub(super) fn named(name: &str) -> Result<Self, RuleXmlError> {
        match name {
            "string" => Ok(Self::String),
            "int" => Ok(Self::Int),
            "long" => Ok(Self::Long),
            "boolean" => Ok(Self::Bool),
            "double" => Ok(Self::Double),
            "dateTime" => Ok(Self::Timestamp),
            _ => Err(RuleXmlError::UnsupportedDefinition),
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Int => "int",
            Self::Long => "long",
            Self::Bool => "boolean",
            Self::Double => "double",
            Self::Timestamp => "dateTime",
        }
    }

    pub(super) fn parse(self, text: String) -> Result<MessageValue, RuleXmlError> {
        if matches!(self, Self::String) {
            return Ok(MessageValue::String(text));
        }
        let value = lexical::trim(&text);
        match self {
            Self::String => unreachable!(),
            Self::Int => value
                .parse::<i32>()
                .map(MessageValue::Int)
                .map_err(|_| RuleXmlError::InvalidDefinition),
            Self::Long => value
                .parse::<i64>()
                .map(MessageValue::Long)
                .map_err(|_| RuleXmlError::InvalidDefinition),
            Self::Bool => match value {
                "true" | "1" => Ok(MessageValue::Bool(true)),
                "false" | "0" => Ok(MessageValue::Bool(false)),
                _ => Err(RuleXmlError::InvalidDefinition),
            },
            Self::Double => match value {
                "NaN" => Err(RuleXmlError::UnsupportedDefinition),
                "INF" => Ok(MessageValue::Double(f64::INFINITY.to_bits())),
                "-INF" => Ok(MessageValue::Double(f64::NEG_INFINITY.to_bits())),
                _ if number(value) => value
                    .parse::<f64>()
                    .map(|value| MessageValue::Double(value.to_bits()))
                    .map_err(|_| RuleXmlError::InvalidDefinition),
                _ => Err(RuleXmlError::InvalidDefinition),
            },
            Self::Timestamp => parse_timestamp(value).map(MessageValue::Timestamp),
        }
    }
}

fn number(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut at = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let start = at;
    while bytes.get(at).is_some_and(u8::is_ascii_digit) {
        at += 1;
    }
    let mut digits = at - start;
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        let start = at;
        while bytes.get(at).is_some_and(u8::is_ascii_digit) {
            at += 1;
        }
        digits += at - start;
    }
    if digits == 0 {
        return false;
    }
    if matches!(bytes.get(at), Some(b'e' | b'E')) {
        at += 1;
        if matches!(bytes.get(at), Some(b'+' | b'-')) {
            at += 1;
        }
        let start = at;
        while bytes.get(at).is_some_and(u8::is_ascii_digit) {
            at += 1;
        }
        if at == start {
            return false;
        }
    }
    at == bytes.len()
}

fn parse_timestamp(value: &str) -> Result<i64, RuleXmlError> {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || &bytes[..4] == b"0000"
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || ![
            &bytes[..4],
            &bytes[5..7],
            &bytes[8..10],
            &bytes[11..13],
            &bytes[14..16],
            &bytes[17..19],
        ]
        .iter()
        .all(|part| part.iter().all(u8::is_ascii_digit))
        || bytes[17] >= b'6'
    {
        return Err(RuleXmlError::InvalidDefinition);
    }
    let mut at = 19;
    // Check precision before Chrono, which ignores fractional digits after nine.
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        let start = at;
        while bytes.get(at).is_some_and(u8::is_ascii_digit) {
            if at - start >= 3 && bytes[at] != b'0' {
                return Err(RuleXmlError::UnsupportedDefinition);
            }
            at += 1;
        }
        if at == start {
            return Err(RuleXmlError::InvalidDefinition);
        }
    }
    let zone = &bytes[at..];
    let explicit_zone = zone == b"Z"
        || if zone.len() == 6
            && matches!(zone[0], b'+' | b'-')
            && zone[3] == b':'
            && [zone[1], zone[2], zone[4], zone[5]]
                .iter()
                .all(u8::is_ascii_digit)
        {
            let hours = (zone[1] - b'0') * 10 + zone[2] - b'0';
            let minutes = (zone[4] - b'0') * 10 + zone[5] - b'0';
            hours <= 14 && minutes < 60 && (hours < 14 || minutes == 0)
        } else {
            false
        };
    if !explicit_zone {
        return Err(RuleXmlError::UnsupportedDefinition);
    }
    let parsed =
        DateTime::parse_from_rfc3339(value).map_err(|_| RuleXmlError::InvalidDefinition)?;
    if !parsed.timestamp_subsec_nanos().is_multiple_of(1_000_000) {
        return Err(RuleXmlError::UnsupportedDefinition);
    }
    let millis = parsed.timestamp_millis();
    timestamp(millis)?;
    Ok(millis)
}

fn timestamp(millis: i64) -> Result<DateTime<Utc>, RuleXmlError> {
    if !(MIN_TIMESTAMP_MILLIS..=MAX_TIMESTAMP_MILLIS).contains(&millis) {
        return Err(RuleXmlError::UnsupportedDefinition);
    }
    DateTime::from_timestamp_millis(millis).ok_or(RuleXmlError::UnsupportedDefinition)
}

pub(super) fn kind(value: &MessageValue) -> Result<ScalarKind, RuleXmlError> {
    match value {
        MessageValue::String(value) if lexical::legal_chars(value) => Ok(ScalarKind::String),
        MessageValue::String(_) => Err(RuleXmlError::UnsupportedDefinition),
        MessageValue::Int(_) => Ok(ScalarKind::Int),
        MessageValue::Long(_) => Ok(ScalarKind::Long),
        MessageValue::Bool(_) => Ok(ScalarKind::Bool),
        MessageValue::Double(bits) if !f64::from_bits(*bits).is_nan() => Ok(ScalarKind::Double),
        MessageValue::Double(_) => Err(RuleXmlError::UnsupportedDefinition),
        MessageValue::Timestamp(millis) => timestamp(*millis).map(|_| ScalarKind::Timestamp),
        MessageValue::Null
        | MessageValue::Ubyte(_)
        | MessageValue::Ushort(_)
        | MessageValue::Uint(_)
        | MessageValue::Ulong(_)
        | MessageValue::Byte(_)
        | MessageValue::Short(_)
        | MessageValue::Float(_)
        | MessageValue::Decimal32(_)
        | MessageValue::Decimal64(_)
        | MessageValue::Decimal128(_)
        | MessageValue::Char(_)
        | MessageValue::Uuid(_)
        | MessageValue::Binary(_)
        | MessageValue::Symbol(_)
        | MessageValue::List(_)
        | MessageValue::Map(_)
        | MessageValue::Array(_)
        | MessageValue::Described { .. } => Err(RuleXmlError::UnsupportedDefinition),
    }
}

pub(super) fn text(value: &MessageValue) -> Result<Cow<'_, str>, RuleXmlError> {
    kind(value)?;
    Ok(match value {
        MessageValue::String(value) => Cow::Borrowed(value),
        MessageValue::Int(value) => Cow::Owned(value.to_string()),
        MessageValue::Long(value) => Cow::Owned(value.to_string()),
        MessageValue::Bool(value) => Cow::Borrowed(if *value { "true" } else { "false" }),
        MessageValue::Double(bits) => {
            let value = f64::from_bits(*bits);
            if value == f64::INFINITY {
                Cow::Borrowed("INF")
            } else if value == f64::NEG_INFINITY {
                Cow::Borrowed("-INF")
            } else {
                Cow::Owned(format!("{value:e}"))
            }
        }
        MessageValue::Timestamp(millis) => Cow::Owned(
            timestamp(*millis)?
                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                .to_string(),
        ),
        _ => return Err(RuleXmlError::UnsupportedDefinition),
    })
}

pub(super) fn validate(filter: &CorrelationFilter) -> Result<(), RuleXmlError> {
    filter
        .validate()
        .map_err(|_| RuleXmlError::InvalidDefinition)?;
    if fields(filter)
        .into_iter()
        .filter_map(|(_, value)| value)
        .any(|value| !lexical::legal_chars(value))
    {
        return Err(RuleXmlError::UnsupportedDefinition);
    }
    let casing = KeyCasing::default();
    let mut keys = BTreeSet::new();
    for (key, value) in &filter.properties {
        if !lexical::legal_chars(key) {
            return Err(RuleXmlError::UnsupportedDefinition);
        }
        kind(value)?;
        if !keys.insert(casing.key(key)) {
            return Err(RuleXmlError::UnsupportedDefinition);
        }
    }
    Ok(())
}

pub(super) fn budget(filter: &CorrelationFilter) -> Result<(usize, usize), RuleXmlError> {
    let mut text_bytes = 0_usize;
    for (_, value) in fields(filter) {
        if let Some(value) = value {
            text_bytes = text_bytes
                .checked_add(value.len())
                .ok_or(RuleXmlError::ReplyLimitExceeded)?;
        }
    }
    for (key, value) in &filter.properties {
        let value_bytes = match kind(value)? {
            ScalarKind::String => match value {
                MessageValue::String(value) => value.len(),
                _ => unreachable!(),
            },
            ScalarKind::Int => 11,
            ScalarKind::Long => 20,
            ScalarKind::Bool => 5,
            ScalarKind::Double | ScalarKind::Timestamp => 24,
        };
        text_bytes = text_bytes
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value_bytes))
            .ok_or(RuleXmlError::ReplyLimitExceeded)?;
    }
    let markup_bytes = filter
        .properties
        .len()
        .checked_mul(PROPERTY_MARKUP_BYTES)
        .ok_or(RuleXmlError::ReplyLimitExceeded)?;
    Ok((text_bytes, markup_bytes))
}
