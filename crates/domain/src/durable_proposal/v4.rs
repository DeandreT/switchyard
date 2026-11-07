//! Frozen send intent carrying each message's original content once, and
//! disposition intent carrying a sparse property patch. Decoding never parses
//! AMQP or applies content policy: invalid content and patch values must
//! survive replication for deterministic refusal. Do not reorder, extend or
//! reuse these DTOs for a later proposal version.

use std::{fmt, marker::PhantomData};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{SeqAccess, Visitor},
};

use crate::{
    Command, CommandKind, EntityBinding, EntityBindingKind, EntityPath, LockToken, MessageContent,
    NamespaceName, OverlayProperty, RawMessage, SequenceNumber, SessionId, Timestamp,
};

use super::{DurableProposal, DurableProposalError as Error, MAX_DURABLE_PROPOSAL_BYTES};

#[derive(Serialize, Deserialize)]
pub(super) struct Proposal {
    authority: Authority,
    namespace: Text,
    entity: Text,
    issued_at: u64,
    kind: Kind,
}

#[derive(Serialize, Deserialize)]
enum Authority {
    Unbound,
    Bound {
        namespace: Text,
        target: Text,
        owner: Text,
        kind: OwnerKind,
        generation: u64,
    },
}

#[derive(Serialize, Deserialize)]
enum OwnerKind {
    Queue,
    Topic,
    Subscription,
}

#[derive(Serialize, Deserialize)]
enum Kind {
    Send(Content),
    SendBatch(Items<Content>),
    Abandon {
        sequence: u64,
        lock_token: u64,
        patch: Items<Entry>,
    },
    Defer {
        sequence: u64,
        lock_token: u64,
        patch: Items<Entry>,
    },
    DeadLetter {
        sequence: u64,
        lock_token: u64,
        reason: Text,
        description: Text,
        patch: Items<Entry>,
    },
}

/// One patch entry in caller order. Repeated names are retained as sent; the
/// state machine coalesces them when it applies the patch.
#[derive(Serialize, Deserialize)]
struct Entry {
    name: Text,
    value: Bytes,
}

#[derive(Serialize, Deserialize)]
enum Content {
    Amqp(Bytes),
    Raw {
        message_id: Text,
        body: Bytes,
        time_to_live_millis: Option<u64>,
        session_id: Option<Text>,
        scheduled_enqueue_at: Option<u64>,
    },
}

#[derive(Serialize)]
#[serde(transparent)]
struct Text(String);

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextVisitor;
        impl<'de> Visitor<'de> for TextVisitor {
            type Value = Text;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded V4 text")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Text, E> {
                if value.len() > MAX_DURABLE_PROPOSAL_BYTES {
                    return Err(E::custom("V4 text exceeds envelope bound"));
                }
                Ok(Text(value.to_owned()))
            }
        }
        deserializer.deserialize_str(TextVisitor)
    }
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
                f.write_str("V4 bytes")
            }
            fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Bytes, E> {
                if value.len() > MAX_DURABLE_PROPOSAL_BYTES {
                    return Err(E::custom("V4 bytes exceed envelope bound"));
                }
                Ok(Bytes(value.to_vec()))
            }
        }
        deserializer.deserialize_byte_buf(Buffer)
    }
}

#[derive(Serialize)]
#[serde(transparent)]
struct Items<T>(Vec<T>);

// Ignore adversarial size hints: allocate only as concrete elements arrive.
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Items<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Elements<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for Elements<T> {
            type Value = Items<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded V4 sequence")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    if values.len() >= MAX_DURABLE_PROPOSAL_BYTES {
                        return Err(serde::de::Error::custom(
                            "V4 sequence exceeds envelope bound",
                        ));
                    }
                    values.push(value);
                }
                Ok(Items(values))
            }
        }
        deserializer.deserialize_seq(Elements(PhantomData))
    }
}

// Every copy charges its encoded length, so capture refuses oversized intent
// before building the whole DTO.
struct CopyBudget(usize);

impl CopyBudget {
    fn charge(&mut self, bytes: usize) -> Result<(), Error> {
        self.0 = self.0.checked_add(bytes).ok_or(Error::TooLarge)?;
        if self.0 > MAX_DURABLE_PROPOSAL_BYTES {
            return Err(Error::TooLarge);
        }
        Ok(())
    }
    fn text(&mut self, value: &str) -> Result<Text, Error> {
        self.charge(value.len())?;
        Ok(Text(value.to_owned()))
    }
    fn bytes(&mut self, value: &[u8]) -> Result<Bytes, Error> {
        self.charge(value.len())?;
        Ok(Bytes(value.to_vec()))
    }
    fn patch(&mut self, patch: &[OverlayProperty]) -> Result<Items<Entry>, Error> {
        patch
            .iter()
            .map(|property| {
                self.charge(1)?;
                Ok(Entry {
                    name: self.text(&property.name)?,
                    value: self.bytes(&property.value)?,
                })
            })
            .collect::<Result<_, _>>()
            .map(Items)
    }
    fn content(&mut self, value: &MessageContent) -> Result<Content, Error> {
        Ok(match value {
            MessageContent::Amqp(bytes) => Content::Amqp(self.bytes(bytes)?),
            MessageContent::Raw(raw) => Content::Raw {
                message_id: self.text(&raw.message_id)?,
                body: self.bytes(&raw.body)?,
                time_to_live_millis: raw.time_to_live_millis,
                session_id: raw
                    .session_id
                    .as_ref()
                    .map(|id| self.text(id.as_str()))
                    .transpose()?,
                scheduled_enqueue_at: raw.scheduled_enqueue_at.map(Timestamp::as_millis),
            },
        })
    }
}

impl Text {
    fn canonical(self) -> Result<String, Error> {
        if self.0.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(Error::NonCanonical);
        }
        Ok(self.0)
    }
    fn namespace(self) -> Result<NamespaceName, Error> {
        NamespaceName::new(self.canonical()?).map_err(|_| Error::InvalidIdentifier)
    }
    fn entity(self) -> Result<EntityPath, Error> {
        EntityPath::from_internal(self.canonical()?).map_err(|_| Error::InvalidIdentifier)
    }
}

impl Content {
    fn restore(self) -> Result<MessageContent, Error> {
        Ok(match self {
            Self::Amqp(bytes) => MessageContent::Amqp(bytes.0),
            Self::Raw {
                message_id,
                body,
                time_to_live_millis,
                session_id,
                scheduled_enqueue_at,
            } => MessageContent::Raw(RawMessage {
                message_id: message_id.0,
                body: body.0,
                time_to_live_millis,
                session_id: session_id
                    .map(|id| SessionId::new(id.0).map_err(|_| Error::InvalidIdentifier))
                    .transpose()?,
                scheduled_enqueue_at: scheduled_enqueue_at.map(Timestamp::from_millis),
            }),
        })
    }
}

fn restore_patch(patch: Items<Entry>) -> Vec<OverlayProperty> {
    patch
        .0
        .into_iter()
        .map(|entry| OverlayProperty {
            name: entry.name.0,
            value: entry.value.0,
        })
        .collect()
}

impl Proposal {
    pub(super) fn capture(value: &DurableProposal) -> Result<Self, Error> {
        let mut budget = CopyBudget(0);
        let kind = match &value.command.kind {
            CommandKind::SendContent { content } => Kind::Send(budget.content(content)?),
            CommandKind::SendContentBatch { messages } => Kind::SendBatch(Items(
                messages
                    .iter()
                    .map(|content| {
                        budget.charge(1)?;
                        budget.content(content)
                    })
                    .collect::<Result<_, _>>()?,
            )),
            CommandKind::AbandonWithPatch {
                sequence,
                lock_token,
                patch,
            } => Kind::Abandon {
                sequence: sequence.as_u64(),
                lock_token: lock_token.as_u64(),
                patch: budget.patch(patch)?,
            },
            CommandKind::DeferWithPatch {
                sequence,
                lock_token,
                patch,
            } => Kind::Defer {
                sequence: sequence.as_u64(),
                lock_token: lock_token.as_u64(),
                patch: budget.patch(patch)?,
            },
            CommandKind::DeadLetterWithPatch {
                sequence,
                lock_token,
                reason,
                description,
                patch,
            } => Kind::DeadLetter {
                sequence: sequence.as_u64(),
                lock_token: lock_token.as_u64(),
                reason: budget.text(reason)?,
                description: budget.text(description)?,
                patch: budget.patch(patch)?,
            },
            _ => return Err(Error::UnsupportedCommandVersion(4)),
        };
        let authority = match value.binding.as_ref() {
            None => Authority::Unbound,
            Some(binding) => {
                // Sends and dispositions keep exactly V1's authority rules. No
                // catalog lookup.
                super::v1::validate_binding(binding, &value.command)?;
                Authority::Bound {
                    namespace: budget.text(binding.namespace().as_str())?,
                    target: budget.text(binding.target().as_str())?,
                    owner: budget.text(binding.owner().as_str())?,
                    kind: match binding.kind() {
                        EntityBindingKind::Queue => OwnerKind::Queue,
                        EntityBindingKind::Topic => OwnerKind::Topic,
                        EntityBindingKind::Subscription => OwnerKind::Subscription,
                    },
                    generation: binding.generation(),
                }
            }
        };
        Ok(Self {
            authority,
            namespace: budget.text(value.command.namespace.as_str())?,
            entity: budget.text(value.command.entity.as_str())?,
            issued_at: value.command.issued_at.as_millis(),
            kind,
        })
    }

    pub(super) fn restore(self) -> Result<DurableProposal, Error> {
        let kind = match self.kind {
            Kind::Send(content) => CommandKind::SendContent {
                content: content.restore()?,
            },
            Kind::SendBatch(messages) => CommandKind::SendContentBatch {
                messages: messages
                    .0
                    .into_iter()
                    .map(Content::restore)
                    .collect::<Result<_, _>>()?,
            },
            Kind::Abandon {
                sequence,
                lock_token,
                patch,
            } => CommandKind::AbandonWithPatch {
                sequence: SequenceNumber::new(sequence),
                lock_token: LockToken::new(lock_token),
                patch: restore_patch(patch),
            },
            Kind::Defer {
                sequence,
                lock_token,
                patch,
            } => CommandKind::DeferWithPatch {
                sequence: SequenceNumber::new(sequence),
                lock_token: LockToken::new(lock_token),
                patch: restore_patch(patch),
            },
            Kind::DeadLetter {
                sequence,
                lock_token,
                reason,
                description,
                patch,
            } => CommandKind::DeadLetterWithPatch {
                sequence: SequenceNumber::new(sequence),
                lock_token: LockToken::new(lock_token),
                reason: reason.0,
                description: description.0,
                patch: restore_patch(patch),
            },
        };
        let command = Command::new(
            self.namespace.namespace()?,
            self.entity.entity()?,
            Timestamp::from_millis(self.issued_at),
            kind,
        );
        let binding = match self.authority {
            Authority::Unbound => None,
            Authority::Bound {
                namespace,
                target,
                owner,
                kind,
                generation,
            } => {
                let binding = EntityBinding::new(
                    namespace.namespace()?,
                    target.entity()?,
                    owner.entity()?,
                    match kind {
                        OwnerKind::Queue => EntityBindingKind::Queue,
                        OwnerKind::Topic => EntityBindingKind::Topic,
                        OwnerKind::Subscription => EntityBindingKind::Subscription,
                    },
                    generation,
                );
                super::v1::validate_binding(&binding, &command)?;
                Some(binding)
            }
        };
        Ok(DurableProposal { command, binding })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal(kind: CommandKind) -> Proposal {
        Proposal::capture(&DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("q").unwrap(),
            Timestamp::UNIX_EPOCH,
            kind,
        )))
        .unwrap()
    }

    fn decode(p: &Proposal) -> Result<DurableProposal, Error> {
        DurableProposal::decode(&super::super::encode_version(p, 4).unwrap())
    }

    #[test]
    fn content_sends_refuse_older_versions_and_legacy_sends_refuse_v4() {
        let p = decode(&proposal(CommandKind::SendContent {
            content: MessageContent::Amqp(vec![0xff]),
        }))
        .unwrap();
        for capture in [
            super::super::v1::Proposal::capture(&p).map(drop),
            super::super::v2::Proposal::capture(&p).map(drop),
            super::super::v3::Proposal::capture(&p).map(drop),
        ] {
            assert!(matches!(capture, Err(Error::UnsupportedCommandVersion(_))));
        }
        let legacy = DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("q").unwrap(),
            Timestamp::UNIX_EPOCH,
            CommandKind::SendBatch { messages: vec![] },
        ));
        assert!(matches!(
            Proposal::capture(&legacy),
            Err(Error::UnsupportedCommandVersion(4))
        ));
    }

    #[test]
    fn malformed_identity_and_raw_sessions_are_never_repaired() {
        for value in ["UPPER", "", "q\0bad"] {
            let mut p = proposal(CommandKind::SendContentBatch { messages: vec![] });
            p.entity = Text(value.into());
            assert!(matches!(
                decode(&p),
                Err(Error::NonCanonical | Error::InvalidIdentifier)
            ));
        }
        let mut p = proposal(CommandKind::SendContentBatch { messages: vec![] });
        p.authority = Authority::Bound {
            namespace: Text("n".into()),
            target: Text("q".into()),
            owner: Text("other".into()),
            kind: OwnerKind::Queue,
            generation: 1,
        };
        assert_eq!(decode(&p), Err(Error::InvalidAuthority));
        let mut p = proposal(CommandKind::SendContentBatch { messages: vec![] });
        p.kind = Kind::Send(Content::Raw {
            message_id: Text("m".into()),
            body: Bytes(vec![]),
            time_to_live_millis: None,
            session_id: Some(Text(String::new())),
            scheduled_enqueue_at: None,
        });
        assert_eq!(decode(&p), Err(Error::InvalidIdentifier));
    }
}
