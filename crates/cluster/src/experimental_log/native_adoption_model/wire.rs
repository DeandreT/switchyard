use std::fmt;

use domain::{CommittedEntryId, CommittedEntryMark};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use super::*;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct Blob<'a, const N: usize>(pub(super) &'a [u8]);

impl<const N: usize> Serialize for Blob<'_, N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        if self.0.len() > N {
            return Err(serde::ser::Error::custom("model byte limit"));
        }
        serializer.serialize_bytes(self.0)
    }
}

impl<'de: 'a, 'a, const N: usize> Deserialize<'de> for Blob<'a, N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct BorrowOnly<const N: usize>;
        impl<'de, const N: usize> de::Visitor<'de> for BorrowOnly<N> {
            type Value = Blob<'de, N>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded borrowed model bytes")
            }
            fn visit_borrowed_bytes<E: de::Error>(
                self,
                bytes: &'de [u8],
            ) -> std::result::Result<Self::Value, E> {
                if bytes.len() > N {
                    return Err(E::custom("model byte limit"));
                }
                Ok(Blob(bytes))
            }
        }
        deserializer.deserialize_bytes(BorrowOnly::<N>)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Member<'a> {
    pub(super) source: CommittedEntryId,
    pub(super) schema: u16,
    #[serde(borrow)]
    pub(super) payload: Blob<'a, MAX_MEMBER>,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct CheckpointFields<'a> {
    pub(super) stream: [u8; 16],
    pub(super) last: Option<CommittedEntryMark>,
    pub(super) previous: Option<CommittedEntryMark>,
    pub(super) timestamp: u64,
    #[serde(borrow)]
    pub(super) membership: Option<Member<'a>>,
}

impl<'a> CheckpointFields<'a> {
    pub(super) fn from_checkpoint(checkpoint: &'a domain::CommittedCheckpoint) -> Self {
        Self {
            stream: *checkpoint.stream().as_bytes(),
            last: checkpoint.last(),
            previous: checkpoint.previous(),
            timestamp: checkpoint.highest_timestamp().as_millis(),
            membership: checkpoint.membership().map(|member| Member {
                source: member.source,
                schema: member.schema_version,
                payload: Blob(&member.payload),
            }),
        }
    }

    pub(super) fn check(self) -> Result<()> {
        domain::CommittedStreamId::new(self.stream).map_err(|_| ModelCodecError::Fields)?;
        let previous_valid = match (self.last, self.previous) {
            (None, None) => self.timestamp == 0 && self.membership.is_none(),
            (Some(last), None) => last.id.index == 0,
            (Some(last), Some(previous)) => {
                previous.id.index.checked_add(1) == Some(last.id.index)
                    && previous.id.term <= last.id.term
            }
            (None, Some(_)) => false,
        };
        let member_valid = self.membership.is_none_or(|member| {
            member.schema != 0
                && member.payload.0.len() <= MAX_MEMBER
                && self.last.is_some_and(|last| {
                    member.source.index <= last.id.index
                        && member.source.term <= last.id.term
                        && (member.source.index != last.id.index || member.source == last.id)
                        && self.previous.is_none_or(|previous| {
                            (member.source.index != previous.id.index
                                || member.source == previous.id)
                                && (member.source.index > previous.id.index
                                    || member.source.term <= previous.id.term)
                        })
                })
        });
        if previous_valid && member_valid {
            Ok(())
        } else {
            Err(ModelCodecError::Fields)
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ImageIdentity<'a> {
    #[serde(borrow)]
    pub(super) checkpoint: Blob<'a, MAX_CHECKPOINT>,
    pub(super) digest: [u8; 32],
    pub(super) bytes: u64,
}

impl ImageIdentity<'_> {
    pub(super) fn fields(&self) -> Result<CheckpointFields<'_>> {
        if self.bytes > MAX_IMAGE as u64 || self.bytes < 60 {
            return Err(ModelCodecError::Fields);
        }
        let fields: CheckpointFields<'_> =
            frame::decode_payload(self.checkpoint.0, MAX_CHECKPOINT)?;
        fields.check()?;
        Ok(fields)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct NativeFields<'a> {
    pub(super) last: Option<CommittedEntryId>,
    pub(super) member_source: Option<CommittedEntryId>,
    pub(super) member_schema: u16,
    #[serde(borrow)]
    pub(super) membership: Blob<'a, MAX_MEMBER>,
    pub(super) snapshot_digest: [u8; 32],
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Vote {
    pub(super) term: u64,
    pub(super) node: u64,
    pub(super) committed: bool,
}

impl Vote {
    pub(super) fn native(self) -> crate::LogVote {
        if self.committed {
            crate::LogVote::new_committed(self.term, self.node)
        } else {
            crate::LogVote::new(self.term, self.node)
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Progress {
    pub(super) vote: Option<Vote>,
    pub(super) purged: Option<CommittedEntryId>,
    pub(super) present: Option<CommittedEntryId>,
    pub(super) entries: u64,
    pub(super) bytes: u64,
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct EntryManifest {
    pub(super) count: u64,
    pub(super) bytes: u64,
    pub(super) digest: [u8; 32],
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum TailChoice {
    ExactContinuation,
    ExactResetEmpty,
}
