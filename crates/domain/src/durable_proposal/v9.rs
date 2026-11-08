//! Frozen Azure-text-ID send intent. V7 carries capacity and V8 filter-error
//! discard policy. V9 composes an exact, canonical V4 send envelope
//! with one 0/1 projection-mode byte per child. It never extends the frozen V4
//! DTOs or parses AMQP. The nested envelope must be V4, so nesting is not recursive.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

use crate::{CommandKind, MessageContent};

use super::{DurableProposal, DurableProposalError as Error, MAX_DURABLE_PROPOSAL_BYTES};

#[derive(Serialize, Deserialize)]
pub(super) struct Proposal {
    legacy_v4: Bytes,
    text_id_children: Bytes,
}

struct Bytes(Vec<u8>);

impl Serialize for Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Buffer;
        impl<'de> Visitor<'de> for Buffer {
            type Value = Bytes;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded V9 bytes")
            }
            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Bytes, E> {
                if bytes.len() > MAX_DURABLE_PROPOSAL_BYTES {
                    return Err(E::custom("V9 bytes exceed envelope bound"));
                }
                Ok(Bytes(bytes.to_vec()))
            }
        }
        deserializer.deserialize_byte_buf(Buffer)
    }
}

pub(super) fn carries(kind: &CommandKind) -> bool {
    match kind {
        CommandKind::SendContent { content } => matches!(content, MessageContent::AmqpTextIds(_)),
        CommandKind::SendContentBatch { messages } => messages
            .iter()
            .any(|content| matches!(content, MessageContent::AmqpTextIds(_))),
        _ => false,
    }
}

fn children(kind: &mut CommandKind) -> Result<&mut [MessageContent], Error> {
    match kind {
        CommandKind::SendContent { content } => Ok(std::slice::from_mut(content)),
        CommandKind::SendContentBatch { messages } => Ok(messages),
        _ => Err(Error::UnsupportedCommandVersion(9)),
    }
}

impl Proposal {
    pub(super) fn capture(proposal: &DurableProposal) -> Result<Self, Error> {
        if !carries(&proposal.command.kind) {
            return Err(Error::UnsupportedCommandVersion(9));
        }
        // Bound the copy before cloning. V4 capture checks all other fields;
        // the final V9 encoder checks framing and nested-envelope overhead.
        let messages = match &proposal.command.kind {
            CommandKind::SendContent { content } => std::slice::from_ref(content),
            CommandKind::SendContentBatch { messages } => messages,
            _ => unreachable!(),
        };
        let mut size = messages.len();
        for message in messages {
            let length = match message {
                MessageContent::Amqp(bytes) | MessageContent::AmqpTextIds(bytes) => bytes.len(),
                MessageContent::Raw(raw) => raw.body.len().saturating_add(raw.message_id.len()),
            };
            size = size.checked_add(length).ok_or(Error::TooLarge)?;
            if size > MAX_DURABLE_PROPOSAL_BYTES {
                return Err(Error::TooLarge);
            }
        }
        let mut legacy = proposal.clone();
        let mut modes = Vec::with_capacity(messages.len());
        for child in children(&mut legacy.command.kind)? {
            let text_ids = matches!(child, MessageContent::AmqpTextIds(_));
            modes.push(u8::from(text_ids));
            if text_ids {
                let MessageContent::AmqpTextIds(bytes) = child else {
                    unreachable!()
                };
                *child = MessageContent::Amqp(std::mem::take(bytes));
            }
        }
        Ok(Self {
            legacy_v4: Bytes(legacy.encode()?),
            text_id_children: Bytes(modes),
        })
    }

    pub(super) fn restore(self) -> Result<DurableProposal, Error> {
        // Check BEFORE recursive entry: only a V4 envelope is ever decoded.
        if self.legacy_v4.0.get(..8) != Some(b"SWDP\0\0\0\x04") {
            return Err(Error::Malformed);
        }
        let mut proposal = DurableProposal::decode(&self.legacy_v4.0)?;
        let messages = children(&mut proposal.command.kind)?;
        if self.text_id_children.0.len() != messages.len() || !self.text_id_children.0.contains(&1)
        {
            return Err(Error::NonCanonical);
        }
        for (message, mode) in messages.iter_mut().zip(self.text_id_children.0) {
            match mode {
                0 => {}
                1 => {
                    let MessageContent::Amqp(bytes) = message else {
                        return Err(Error::Malformed);
                    };
                    *message = MessageContent::AmqpTextIds(std::mem::take(bytes));
                }
                _ => return Err(Error::NonCanonical),
            }
        }
        Ok(proposal)
    }
}

#[cfg(test)]
#[path = "v9_tests.rs"]
mod tests;
