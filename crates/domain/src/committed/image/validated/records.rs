use std::{fmt, io};

use serde::{Deserialize, Serialize, de};

use super::{CommittedImageValidationError as Error, Result, keys};

// This role deliberately freezes version 11; general migration decoding is not
// the image validation boundary.
const VALUE_FORMAT: u8 = 11;

pub(super) fn decode<'a, T>(value: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a> + Serialize,
{
    let (&version, payload) = value.split_first().ok_or(Error::InvalidRecord)?;
    if version != VALUE_FORMAT {
        return Err(Error::UnsupportedProfile);
    }
    let (record, remaining) =
        postcard::take_from_bytes(payload).map_err(|_| Error::InvalidRecord)?;
    if !remaining.is_empty() {
        return Err(Error::InvalidRecord);
    }
    let mut comparison = Comparison { remaining: payload };
    postcard::to_io(&record, &mut comparison).map_err(|_| Error::InvalidRecord)?;
    if !comparison.remaining.is_empty() {
        return Err(Error::InvalidRecord);
    }
    Ok(record)
}

pub(super) fn empty_index(value: &[u8]) -> Result<()> {
    if !value.is_empty() {
        return Err(Error::InconsistentIndex);
    }
    Ok(())
}

#[derive(Deserialize, Serialize)]
pub(super) struct MessageV11<'a> {
    pub(super) sequence: u64,
    pub(super) message_id: &'a str,
    pub(super) body: &'a [u8],
    pub(super) enqueued_at: u64,
    pub(super) expires_at: Option<u64>,
    delivery_count: u32,
    state: ReadyOnly,
    #[serde(borrow)]
    pub(super) session_id: Option<&'a str>,
    dead_letter: NoneOnly,
    scheduled_enqueue_time: NoneOnly,
    envelope: NoneOnly,
}

#[derive(Deserialize, Serialize)]
enum ReadyOnly {
    Ready,
}

struct NoneOnly;

impl Serialize for NoneOnly {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_none()
    }
}

impl<'de> Deserialize<'de> for NoneOnly {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = NoneOnly;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("absent unsupported metadata")
            }

            fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(NoneOnly)
            }

            fn visit_some<D: serde::Deserializer<'de>>(
                self,
                _: D,
            ) -> std::result::Result<Self::Value, D::Error> {
                Err(de::Error::custom("unsupported metadata"))
            }
        }
        deserializer.deserialize_option(Visitor)
    }
}

pub(super) fn message(value: &[u8]) -> Result<MessageV11<'_>> {
    supported_message_shape(value)?;
    let message: MessageV11<'_> = decode(value)?;
    if message.sequence == 0
        || message.sequence > crate::MAX_SEQUENCE_NUMBER
        || message.message_id.encode_utf16().count() > crate::MAX_MESSAGE_ID_LENGTH
        || message.body.len() > crate::MAX_COMMITTED_BODY_BYTES
        || message
            .expires_at
            .is_some_and(|deadline| deadline < message.enqueued_at)
    {
        return Err(Error::InvalidRecord);
    }
    if let Some(session) = message.session_id {
        keys::identifier(session, crate::MAX_SESSION_ID_BYTES)?;
    }
    Ok(message)
}

// Recognize broader states/options without decoding their potentially large
// contents, and without conflating a supported broader store with corruption.
// The complete supported mirror is still canonically checked afterward.
fn supported_message_shape(value: &[u8]) -> Result<()> {
    #[derive(Deserialize)]
    struct Prefix<'a> {
        _sequence: u64,
        _message_id: &'a str,
        _body: &'a [u8],
        _enqueued_at: u64,
        _expires_at: Option<u64>,
        delivery_count: u32,
    }
    #[derive(Deserialize)]
    struct Session<'a> {
        #[serde(borrow)]
        _session_id: Option<&'a str>,
    }
    let (&version, payload) = value.split_first().ok_or(Error::InvalidRecord)?;
    if version != VALUE_FORMAT {
        return Err(Error::UnsupportedProfile);
    }
    let (prefix, rest): (Prefix<'_>, _) =
        postcard::take_from_bytes(payload).map_err(|_| Error::InvalidRecord)?;
    if prefix.delivery_count != 0 {
        return Err(Error::UnsupportedProfile);
    }
    // Postcard's enum discriminator is the same unsigned varint as u32. Stop
    // before variant data for every recognized broader MessageState.
    let (state, rest): (u32, _) =
        postcard::take_from_bytes(rest).map_err(|_| Error::InvalidRecord)?;
    match state {
        0 => {}
        1..=3 => return Err(Error::UnsupportedProfile),
        _ => return Err(Error::InvalidRecord),
    }
    let (_, mut rest): (Session<'_>, _) =
        postcard::take_from_bytes(rest).map_err(|_| Error::InvalidRecord)?;
    for _ in 0..3 {
        // Option discriminants are strict 0/1 bytes; bool has that same encoding.
        let (present, tail): (bool, _) =
            postcard::take_from_bytes(rest).map_err(|_| Error::InvalidRecord)?;
        if present {
            return Err(Error::UnsupportedProfile);
        }
        rest = tail;
    }
    if !rest.is_empty() {
        return Err(Error::InvalidRecord);
    }
    Ok(())
}

struct Comparison<'a> {
    remaining: &'a [u8],
}

impl io::Write for Comparison<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.remaining.starts_with(bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "noncanonical record",
            ));
        }
        self.remaining = &self.remaining[bytes.len()..];
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
