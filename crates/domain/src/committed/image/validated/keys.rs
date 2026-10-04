use crate::{
    DEAD_LETTER_QUEUE_SUFFIX, MAX_ENTITY_PATH_BYTES, MAX_NAMESPACE_NAME_BYTES,
    MAX_SESSION_ID_BYTES, SUBSCRIPTION_PATH_SEGMENT,
};

use super::{CommittedImageValidationError as Error, Result};

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct Scope<'a> {
    pub(super) namespace: &'a str,
    pub(super) entity: &'a str,
}

impl Scope<'_> {
    pub(super) fn is_shadow(self) -> bool {
        self.entity
            .as_bytes()
            .get(
                self.entity
                    .len()
                    .saturating_sub(DEAD_LETTER_QUEUE_SUFFIX.len())..,
            )
            .is_some_and(|tail| tail.eq_ignore_ascii_case(DEAD_LETTER_QUEUE_SUFFIX.as_bytes()))
    }

    pub(super) fn is_primary(self) -> bool {
        !self.is_shadow()
            && !self
                .entity
                .as_bytes()
                .windows(SUBSCRIPTION_PATH_SEGMENT.len())
                .any(|part| part.eq_ignore_ascii_case(SUBSCRIPTION_PATH_SEGMENT.as_bytes()))
            && self.entity.len() + DEAD_LETTER_QUEUE_SUFFIX.len() <= MAX_ENTITY_PATH_BYTES
    }
}

pub(super) enum Key<'a> {
    Clock,
    Checkpoint,
    Config(Scope<'a>),
    Counters(Scope<'a>),
    Incarnation(Scope<'a>),
    Message(Scope<'a>, u64),
    Ready(Scope<'a>, u64),
    Expiry(Scope<'a>, u64, u64),
    SessionReady(Scope<'a>, &'a str, u64),
    History(Scope<'a>, &'a str),
    HistoryExpiry(Scope<'a>, u64, &'a str),
}

impl<'a> Key<'a> {
    pub(super) fn parse(key: &'a [u8]) -> Result<Self> {
        let (&tag, rest) = key.split_first().ok_or(Error::InvalidKey)?;
        if matches!(tag, 0x00 | 0x12) {
            if !rest.is_empty() {
                return Err(Error::InvalidKey);
            }
            return Ok(if tag == 0 {
                Self::Clock
            } else {
                Self::Checkpoint
            });
        }
        if !matches!(
            tag,
            0x01 | 0x02 | 0x03 | 0x04 | 0x06 | 0x09 | 0x0c | 0x0d | 0x11
        ) {
            return Err(Error::UnsupportedProfile);
        }
        let (namespace, rest) = segment(rest, MAX_NAMESPACE_NAME_BYTES)?;
        let (entity, tail) = segment(rest, MAX_ENTITY_PATH_BYTES)?;
        let scope = Scope { namespace, entity };
        Ok(match tag {
            0x01 | 0x02 | 0x11 => {
                if !tail.is_empty() {
                    return Err(Error::InvalidKey);
                }
                match tag {
                    0x01 => Self::Config(scope),
                    0x02 => Self::Counters(scope),
                    _ => Self::Incarnation(scope),
                }
            }
            0x03 | 0x04 => {
                let sequence = sequence(tail)?;
                if tag == 0x03 {
                    Self::Message(scope, sequence)
                } else {
                    Self::Ready(scope, sequence)
                }
            }
            0x06 => {
                if tail.len() != 16 {
                    return Err(Error::InvalidKey);
                }
                Self::Expiry(scope, number(&tail[..8])?, sequence(&tail[8..])?)
            }
            0x09 => {
                let (session, tail) = segment(tail, MAX_SESSION_ID_BYTES)?;
                Self::SessionReady(scope, session, sequence(tail)?)
            }
            0x0c => Self::History(scope, message_id(tail)?),
            0x0d => {
                let deadline = number(tail.get(..8).ok_or(Error::InvalidKey)?)?;
                Self::HistoryExpiry(scope, deadline, message_id(&tail[8..])?)
            }
            _ => return Err(Error::InvalidKey),
        })
    }
}

fn segment(bytes: &[u8], maximum: usize) -> Result<(&str, &[u8])> {
    let end = bytes
        .iter()
        .position(|&byte| byte == 0)
        .ok_or(Error::InvalidKey)?;
    let text = std::str::from_utf8(&bytes[..end]).map_err(|_| Error::InvalidKey)?;
    identifier(text, maximum).map_err(|_| Error::InvalidKey)?;
    Ok((text, &bytes[end + 1..]))
}

pub(super) fn identifier(text: &str, maximum: usize) -> Result<()> {
    if text.is_empty() || text.len() > maximum || text.chars().any(char::is_control) {
        return Err(Error::InvalidRecord);
    }
    Ok(())
}

fn message_id(bytes: &[u8]) -> Result<&str> {
    let text = std::str::from_utf8(bytes).map_err(|_| Error::InvalidKey)?;
    if text.is_empty() || text.encode_utf16().count() > crate::MAX_MESSAGE_ID_LENGTH {
        return Err(Error::InvalidKey);
    }
    // The whole remainder belongs to the ID, including embedded NUL bytes.
    Ok(text)
}

fn number(bytes: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        bytes.try_into().map_err(|_| Error::InvalidKey)?,
    ))
}

fn sequence(bytes: &[u8]) -> Result<u64> {
    let value = number(bytes)?;
    if value == 0 || value > crate::MAX_SEQUENCE_NUMBER {
        return Err(Error::InvalidKey);
    }
    Ok(value)
}
