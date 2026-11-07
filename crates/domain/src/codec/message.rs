//! Frozen message records. V1 reproduces exactly the bytes the generic value
//! codec wrote for `MessageRecord`. V2 stores either raw bytes or the original
//! AMQP message with a copy-local property overlay, without a duplicated body.
//! Content sends produce V2; every rewrite keeps the record's source format,
//! except a property patch, which converts a V1 record to V2.

use std::{borrow::Cow, collections::BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    CorrelationValue, DeadLetterInfo, DeadLetterReason, DeliveryOrigin, FilterProperties,
    LockToken, MessageEnvelope, MessageRecord, MessageRecordFormat, MessageState, OverlayProperty,
    SequenceNumber, SessionId, Timestamp,
};

use super::{CodecError, VALUE_FORMAT_V1};

pub const VALUE_FORMAT_V2: u8 = 2;

// Stored DTOs are frozen independently of the public types' serde: do not
// reorder, extend or reuse them for a later version. `Cow` borrows the record
// while encoding and owns the data after decoding.
#[derive(Serialize, Deserialize)]
struct RecordV1<'a> {
    sequence: u64,
    message_id: Cow<'a, str>,
    body: Cow<'a, [u8]>,
    enqueued_at: u64,
    scheduled_enqueue_at: Option<u64>,
    expires_at: Option<u64>,
    delivery_count: u32,
    state: State,
    session_id: Option<Cow<'a, str>>,
    dead_letter: Option<DeadLetter<'a>>,
    envelope: Option<EnvelopeV1<'a>>,
}

#[derive(Serialize, Deserialize)]
struct RecordV2<'a> {
    sequence: u64,
    message_id: Cow<'a, str>,
    enqueued_at: u64,
    scheduled_enqueue_at: Option<u64>,
    expires_at: Option<u64>,
    delivery_count: u32,
    state: State,
    session_id: Option<Cow<'a, str>>,
    dead_letter: Option<DeadLetter<'a>>,
    content: Content<'a>,
}

#[derive(Serialize, Deserialize)]
enum State {
    Ready,
    Deferred,
    Scheduled,
    Locked {
        token: u64,
        locked_until: u64,
        origin: Origin,
    },
}

#[derive(Serialize, Deserialize)]
enum Origin {
    Ready,
    Deferred,
    Scheduled,
}

#[derive(Serialize, Deserialize)]
struct DeadLetter<'a> {
    reason: Reason<'a>,
    description: Cow<'a, str>,
    dead_lettered_at: u64,
}

#[derive(Serialize, Deserialize)]
enum Reason<'a> {
    MaxDeliveryCountExceeded,
    TimeToLiveExpired,
    Application(Cow<'a, str>),
}

#[derive(Serialize, Deserialize)]
struct EnvelopeV1<'a> {
    bytes: Cow<'a, [u8]>,
    filter_properties: Projection<'a>,
}

// Postcard frames a map exactly like a sequence of key/value pairs, so the
// pairs reproduce the former `BTreeMap` bytes.
#[derive(Serialize, Deserialize)]
struct Projection<'a> {
    correlation_id: Option<Cow<'a, str>>,
    message_id: Option<Cow<'a, str>>,
    to: Option<Cow<'a, str>>,
    reply_to: Option<Cow<'a, str>>,
    subject: Option<Cow<'a, str>>,
    session_id: Option<Cow<'a, str>>,
    reply_to_session_id: Option<Cow<'a, str>>,
    content_type: Option<Cow<'a, str>>,
    application_properties: Vec<(Cow<'a, str>, Cow<'a, [u8]>)>,
}

#[derive(Serialize, Deserialize)]
enum Content<'a> {
    Raw(Cow<'a, [u8]>),
    Amqp {
        original: Cow<'a, [u8]>,
        overlay: Vec<Entry<'a>>,
        projection: Box<Projection<'a>>,
    },
}

/// One overlay value is one encoded AMQP value; `0x40` is an explicit null.
/// The codec checks only its framing and never parses AMQP.
#[derive(Serialize, Deserialize)]
struct Entry<'a> {
    name: Cow<'a, str>,
    value: Cow<'a, [u8]>,
}

/// Encodes a record in the format it was created or read with. V1 cannot carry
/// an overlay; V2 refuses an AMQP record that also carries a separate body,
/// duplicate overlay names and empty overlay values.
pub fn encode(record: &MessageRecord) -> Result<Vec<u8>, CodecError> {
    match record.format {
        MessageRecordFormat::V1 => super::encode(&RecordV1 {
            sequence: record.sequence.as_u64(),
            message_id: Cow::Borrowed(&record.message_id),
            body: Cow::Borrowed(&record.body),
            enqueued_at: record.enqueued_at.as_millis(),
            scheduled_enqueue_at: record.scheduled_enqueue_at.map(Timestamp::as_millis),
            expires_at: record.expires_at.map(Timestamp::as_millis),
            delivery_count: record.delivery_count,
            state: State::capture(&record.state),
            session_id: record
                .session_id
                .as_ref()
                .map(|id| Cow::Borrowed(id.as_str())),
            dead_letter: record.dead_letter.as_ref().map(DeadLetter::capture),
            envelope: record
                .envelope
                .as_ref()
                .map(|envelope| {
                    if !envelope.overlay().is_empty() {
                        return Err(CodecError::Encode);
                    }
                    Ok(EnvelopeV1 {
                        bytes: Cow::Borrowed(envelope.as_bytes()),
                        filter_properties: Projection::capture(envelope.filter_properties()),
                    })
                })
                .transpose()?,
        }),
        MessageRecordFormat::V2 => {
            let content = match &record.envelope {
                None => Content::Raw(Cow::Borrowed(&record.body)),
                Some(envelope) => {
                    if !record.body.is_empty() {
                        return Err(CodecError::Encode);
                    }
                    Content::Amqp {
                        original: Cow::Borrowed(envelope.as_bytes()),
                        overlay: capture_overlay(envelope.overlay())?,
                        projection: Box::new(Projection::capture(envelope.filter_properties())),
                    }
                }
            };
            let mut bytes = vec![VALUE_FORMAT_V2];
            bytes.extend(
                postcard::to_stdvec(&RecordV2 {
                    sequence: record.sequence.as_u64(),
                    message_id: Cow::Borrowed(&record.message_id),
                    enqueued_at: record.enqueued_at.as_millis(),
                    scheduled_enqueue_at: record.scheduled_enqueue_at.map(Timestamp::as_millis),
                    expires_at: record.expires_at.map(Timestamp::as_millis),
                    delivery_count: record.delivery_count,
                    state: State::capture(&record.state),
                    session_id: record
                        .session_id
                        .as_ref()
                        .map(|id| Cow::Borrowed(id.as_str())),
                    dead_letter: record.dead_letter.as_ref().map(DeadLetter::capture),
                    content,
                })
                .map_err(|_| CodecError::Encode)?,
            );
            Ok(bytes)
        }
    }
}

/// Runtime reads keep the generic codec's tolerance of bytes after a V1
/// record. V2 framing is exact.
pub fn decode(bytes: &[u8]) -> Result<MessageRecord, CodecError> {
    let (record, unused) = read(bytes)?;
    if record.format == MessageRecordFormat::V2 && !unused.is_empty() {
        return Err(CodecError::Decode);
    }
    Ok(record)
}

/// Why a stored row is not an exact record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CanonicalError {
    /// The row cannot be read as a record of any supported version.
    Codec(CodecError),
    /// The row decodes, but trailing bytes, nonminimal integers or unordered or
    /// duplicate projection keys mean re-encoding would not reproduce it.
    NonCanonical,
}

/// Exact framing and re-encoding reject trailing, nonminimal and otherwise
/// noncanonical rows of either version. A V1 row is never upgraded.
pub(crate) fn decode_canonical(bytes: &[u8]) -> Result<MessageRecord, CanonicalError> {
    let (record, unused) = read(bytes).map_err(CanonicalError::Codec)?;
    if !unused.is_empty() || encode(&record).map_err(CanonicalError::Codec)? != bytes {
        return Err(CanonicalError::NonCanonical);
    }
    Ok(record)
}

fn read(bytes: &[u8]) -> Result<(MessageRecord, &[u8]), CodecError> {
    let (&version, payload) = bytes.split_first().ok_or(CodecError::EmptyEnvelope)?;
    match version {
        VALUE_FORMAT_V1 => {
            let (stored, unused) =
                postcard::take_from_bytes::<RecordV1>(payload).map_err(|_| CodecError::Decode)?;
            let envelope = stored.envelope.map(|envelope| {
                MessageEnvelope::new(envelope.bytes.into_owned())
                    .with_filter_properties(envelope.filter_properties.restore())
            });
            let record = MessageRecord {
                sequence: SequenceNumber::new(stored.sequence),
                message_id: stored.message_id.into_owned(),
                body: stored.body.into_owned(),
                enqueued_at: Timestamp::from_millis(stored.enqueued_at),
                scheduled_enqueue_at: stored.scheduled_enqueue_at.map(Timestamp::from_millis),
                expires_at: stored.expires_at.map(Timestamp::from_millis),
                delivery_count: stored.delivery_count,
                state: stored.state.restore(),
                session_id: restore_session(stored.session_id)?,
                dead_letter: stored.dead_letter.map(DeadLetter::restore),
                envelope,
                format: MessageRecordFormat::V1,
            };
            Ok((record, unused))
        }
        VALUE_FORMAT_V2 => {
            let (stored, unused) =
                postcard::take_from_bytes::<RecordV2>(payload).map_err(|_| CodecError::Decode)?;
            let (body, envelope) = match stored.content {
                Content::Raw(bytes) => (bytes.into_owned(), None),
                Content::Amqp {
                    original,
                    overlay,
                    projection,
                } => (
                    Vec::new(),
                    Some(
                        MessageEnvelope::new(original.into_owned())
                            .with_filter_properties(projection.restore())
                            .with_overlay(restore_overlay(overlay)?),
                    ),
                ),
            };
            let record = MessageRecord {
                sequence: SequenceNumber::new(stored.sequence),
                message_id: stored.message_id.into_owned(),
                body,
                enqueued_at: Timestamp::from_millis(stored.enqueued_at),
                scheduled_enqueue_at: stored.scheduled_enqueue_at.map(Timestamp::from_millis),
                expires_at: stored.expires_at.map(Timestamp::from_millis),
                delivery_count: stored.delivery_count,
                state: stored.state.restore(),
                session_id: restore_session(stored.session_id)?,
                dead_letter: stored.dead_letter.map(DeadLetter::restore),
                envelope,
                format: MessageRecordFormat::V2,
            };
            Ok((record, unused))
        }
        version => Err(CodecError::UnsupportedVersion { version }),
    }
}

impl State {
    fn capture(value: &MessageState) -> Self {
        match value {
            MessageState::Ready => Self::Ready,
            MessageState::Deferred => Self::Deferred,
            MessageState::Scheduled => Self::Scheduled,
            MessageState::Locked {
                token,
                locked_until,
                origin,
            } => Self::Locked {
                token: token.as_u64(),
                locked_until: locked_until.as_millis(),
                origin: match origin {
                    DeliveryOrigin::Ready => Origin::Ready,
                    DeliveryOrigin::Deferred => Origin::Deferred,
                    DeliveryOrigin::Scheduled => Origin::Scheduled,
                },
            },
        }
    }

    fn restore(self) -> MessageState {
        match self {
            Self::Ready => MessageState::Ready,
            Self::Deferred => MessageState::Deferred,
            Self::Scheduled => MessageState::Scheduled,
            Self::Locked {
                token,
                locked_until,
                origin,
            } => MessageState::Locked {
                token: LockToken::new(token),
                locked_until: Timestamp::from_millis(locked_until),
                origin: match origin {
                    Origin::Ready => DeliveryOrigin::Ready,
                    Origin::Deferred => DeliveryOrigin::Deferred,
                    Origin::Scheduled => DeliveryOrigin::Scheduled,
                },
            },
        }
    }
}

impl<'a> DeadLetter<'a> {
    fn capture(value: &'a DeadLetterInfo) -> Self {
        Self {
            reason: match &value.reason {
                DeadLetterReason::MaxDeliveryCountExceeded => Reason::MaxDeliveryCountExceeded,
                DeadLetterReason::TimeToLiveExpired => Reason::TimeToLiveExpired,
                DeadLetterReason::Application(reason) => Reason::Application(Cow::Borrowed(reason)),
            },
            description: Cow::Borrowed(&value.description),
            dead_lettered_at: value.dead_lettered_at.as_millis(),
        }
    }

    fn restore(self) -> DeadLetterInfo {
        DeadLetterInfo {
            reason: match self.reason {
                Reason::MaxDeliveryCountExceeded => DeadLetterReason::MaxDeliveryCountExceeded,
                Reason::TimeToLiveExpired => DeadLetterReason::TimeToLiveExpired,
                Reason::Application(reason) => DeadLetterReason::Application(reason.into_owned()),
            },
            description: self.description.into_owned(),
            dead_lettered_at: Timestamp::from_millis(self.dead_lettered_at),
        }
    }
}

impl<'a> Projection<'a> {
    fn capture(value: &'a FilterProperties) -> Self {
        let text = |value: &'a Option<String>| value.as_deref().map(Cow::Borrowed);
        Self {
            correlation_id: text(&value.correlation_id),
            message_id: text(&value.message_id),
            to: text(&value.to),
            reply_to: text(&value.reply_to),
            subject: text(&value.subject),
            session_id: text(&value.session_id),
            reply_to_session_id: text(&value.reply_to_session_id),
            content_type: text(&value.content_type),
            application_properties: value
                .application_properties
                .iter()
                .map(|(name, value)| {
                    (
                        Cow::Borrowed(name.as_str()),
                        Cow::Borrowed(value.as_bytes()),
                    )
                })
                .collect(),
        }
    }

    /// Later duplicates replace earlier ones, as the former map decoder did;
    /// canonical decoding refuses them through re-encoding.
    fn restore(self) -> FilterProperties {
        let text = |value: Option<Cow<'a, str>>| value.map(Cow::into_owned);
        FilterProperties {
            correlation_id: text(self.correlation_id),
            message_id: text(self.message_id),
            to: text(self.to),
            reply_to: text(self.reply_to),
            subject: text(self.subject),
            session_id: text(self.session_id),
            reply_to_session_id: text(self.reply_to_session_id),
            content_type: text(self.content_type),
            application_properties: self
                .application_properties
                .into_iter()
                .map(|(name, value)| {
                    (
                        name.into_owned(),
                        CorrelationValue::from_durable_bytes(value.into_owned()),
                    )
                })
                .collect(),
        }
    }
}

fn capture_overlay(overlay: &[OverlayProperty]) -> Result<Vec<Entry<'_>>, CodecError> {
    let mut names = BTreeSet::new();
    overlay
        .iter()
        .map(|property| {
            if property.value.is_empty() || !names.insert(property.name.as_str()) {
                return Err(CodecError::Encode);
            }
            Ok(Entry {
                name: Cow::Borrowed(&property.name),
                value: Cow::Borrowed(&property.value),
            })
        })
        .collect()
}

fn restore_overlay(overlay: Vec<Entry<'_>>) -> Result<Vec<OverlayProperty>, CodecError> {
    let mut names = BTreeSet::new();
    overlay
        .into_iter()
        .map(|entry| {
            if entry.value.is_empty() || !names.insert(entry.name.clone()) {
                return Err(CodecError::Decode);
            }
            Ok(OverlayProperty {
                name: entry.name.into_owned(),
                value: entry.value.into_owned(),
            })
        })
        .collect()
}

fn restore_session(value: Option<Cow<'_, str>>) -> Result<Option<SessionId>, CodecError> {
    value
        .map(|id| SessionId::new(id.into_owned()).map_err(|_| CodecError::Decode))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(format: MessageRecordFormat) -> MessageRecord {
        MessageRecord {
            sequence: SequenceNumber::new(1),
            message_id: String::from("m-1"),
            body: vec![1],
            enqueued_at: Timestamp::from_millis(0),
            scheduled_enqueue_at: None,
            expires_at: None,
            delivery_count: 0,
            state: MessageState::Ready,
            session_id: None,
            dead_letter: None,
            envelope: None,
            format,
        }
    }

    #[test]
    fn canonical_decoding_keeps_the_version_and_refuses_trailing_bytes() {
        for format in [MessageRecordFormat::V1, MessageRecordFormat::V2] {
            let bytes = encode(&record(format)).expect("encodes");
            assert_eq!(decode_canonical(&bytes), Ok(record(format)));
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert_eq!(
                decode_canonical(&trailing),
                Err(CanonicalError::NonCanonical)
            );
        }
        assert_eq!(
            decode_canonical(&[]),
            Err(CanonicalError::Codec(CodecError::EmptyEnvelope))
        );
        assert_eq!(
            decode_canonical(&[3]),
            Err(CanonicalError::Codec(CodecError::UnsupportedVersion {
                version: 3
            }))
        );
    }

    fn unhex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex"))
            .collect()
    }

    // A locked V1 record whose envelope projects application properties a and b.
    const PROJECTED: &str = "010b046d2d313100000000020307d209010103732d3100010400537740\
                             01016301016d0101740101720101730103732d310102727301026374";

    #[test]
    fn canonical_decoding_refuses_decodable_but_noncanonical_v1_rows() {
        let canonical = unhex(&format!("{PROJECTED}02016101400162025205"));
        assert!(decode_canonical(&canonical).is_ok());
        let mut nonminimal = canonical.clone();
        nonminimal.splice(1..2, [0x8b, 0x00]);
        for (label, bytes) in [
            ("nonminimal varint", nonminimal),
            (
                "unordered projection keys",
                unhex(&format!("{PROJECTED}02016202520501610140")),
            ),
            (
                "duplicate projection keys",
                unhex(&format!("{PROJECTED}02016101400161025205")),
            ),
        ] {
            assert!(decode(&bytes).is_ok(), "{label} stays readable at runtime");
            assert_eq!(
                decode_canonical(&bytes),
                Err(CanonicalError::NonCanonical),
                "{label}"
            );
        }
    }
}
