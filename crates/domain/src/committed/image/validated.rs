//! Pure consistency validation for the closed, current Create/Send image role.
//!
//! This does not certify a source, committed history, membership, ancestry, or
//! authority to export or install the bytes. No store or clock is accessed.

use std::{collections::BTreeMap, fmt};

use crate::{
    CommittedCheckpoint, CommittedStreamId, EntityIncarnation, QueueConfig, QueueCounters,
};

use super::{CommittedImageRole, CommittedImageRows, DecodedCommittedImage};

mod keys;
mod records;
mod relations;

#[cfg(test)]
mod tests;

use keys::{Key, Scope};
use records::MessageV11;

/// Static consistency failures; supplied rows and identifiers are never retained.
///
/// `UnsupportedProfile` does not imply corruption. Relational refusal also
/// does not prove the source only ever ran this role: broader operations can
/// leave apparently missing history or otherwise out-of-profile rows. This
/// pure result is not by itself authority to poison a source owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CommittedImageValidationError {
    #[error("committed image business profile is unsupported")]
    UnsupportedProfile,
    #[error("committed image business key is invalid")]
    InvalidKey,
    #[error("committed image business record is invalid")]
    InvalidRecord,
    #[error("committed image queue metadata is inconsistent")]
    InconsistentMetadata,
    #[error("committed image retained message is inconsistent")]
    InconsistentMessage,
    #[error("committed image message index is inconsistent")]
    InconsistentIndex,
    #[error("committed image duplicate history is inconsistent")]
    InconsistentHistory,
    #[error("committed image business clock is inconsistent")]
    InvalidClock,
}

type Result<T> = std::result::Result<T, CommittedImageValidationError>;

/// Immutable checked view of one complete declared CreateSendV1 image.
///
/// Business bodies and names remain borrowed. Temporary metadata collections
/// are bounded by the container's row count, not by an RSS guarantee. Sequence
/// holes and expiry deadlines are checked for consistency, not reconstructed
/// into historical requests. Membership bytes remain opaque.
///
/// ```compile_fail
/// fn duplicate(image: domain::ValidatedCreateSendImage<'_>) {
///     let _ = image.clone();
/// }
/// ```
pub struct ValidatedCreateSendImage<'a> {
    image: DecodedCommittedImage<'a>,
    primary_queues: usize,
    messages: usize,
}

impl<'a> ValidatedCreateSendImage<'a> {
    pub fn validate(image: DecodedCommittedImage<'a>) -> Result<Self> {
        match image.role() {
            CommittedImageRole::CreateSendV1 => {}
        }
        let mut rows = Rows::default();
        for row in image.rows() {
            rows.insert(Key::parse(row.key())?, row.value())?;
        }
        let primary_queues = relations::validate(&rows, image.checkpoint())?;
        Ok(Self {
            image,
            primary_queues,
            messages: rows.messages.len(),
        })
    }

    pub fn checkpoint(&self) -> &CommittedCheckpoint {
        self.image.checkpoint()
    }

    pub fn stream(&self) -> CommittedStreamId {
        self.image.stream()
    }

    pub fn row_count(&self) -> usize {
        self.image.row_count()
    }

    /// Counts primary queues only; their required shadows are not extra queues.
    pub fn queue_count(&self) -> usize {
        self.primary_queues
    }

    pub fn message_count(&self) -> usize {
        self.messages
    }

    pub fn rows(&self) -> CommittedImageRows<'a> {
        self.image.rows()
    }
}

impl fmt::Debug for ValidatedCreateSendImage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedCreateSendImage")
            .field("rows", &self.row_count())
            .field("primary_queues", &self.primary_queues)
            .field("messages", &self.messages)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct Rows<'a> {
    clock: Option<u64>,
    configs: BTreeMap<Scope<'a>, QueueConfig>,
    counters: BTreeMap<Scope<'a>, QueueCounters>,
    incarnations: BTreeMap<Scope<'a>, EntityIncarnation>,
    messages: BTreeMap<(Scope<'a>, u64), MessageV11<'a>>,
    ready: BTreeMap<(Scope<'a>, u64), Option<&'a str>>,
    expiry: BTreeMap<(Scope<'a>, u64), u64>,
    history: BTreeMap<(Scope<'a>, &'a str), u64>,
    history_expiry: BTreeMap<(Scope<'a>, &'a str), u64>,
}

impl<'a> Rows<'a> {
    fn insert(&mut self, key: Key<'a>, value: &'a [u8]) -> Result<()> {
        use CommittedImageValidationError::{InconsistentHistory, InconsistentIndex};
        match key {
            Key::Checkpoint => {}
            Key::Clock => self.clock = Some(records::decode(value)?),
            Key::Config(scope) => {
                let config: QueueConfig = records::decode(value)?;
                config
                    .validate()
                    .map_err(|_| CommittedImageValidationError::InvalidRecord)?;
                self.configs.insert(scope, config);
            }
            Key::Counters(scope) => {
                self.counters.insert(scope, records::decode(value)?);
            }
            Key::Incarnation(scope) => {
                self.incarnations.insert(scope, records::decode(value)?);
            }
            Key::Message(scope, sequence) => {
                let message = records::message(value)?;
                if message.sequence != sequence {
                    return Err(CommittedImageValidationError::InconsistentMessage);
                }
                self.messages.insert((scope, sequence), message);
            }
            Key::Ready(scope, sequence) => {
                records::empty_index(value)?;
                if self.ready.insert((scope, sequence), None).is_some() {
                    return Err(InconsistentIndex);
                }
            }
            Key::SessionReady(scope, session, sequence) => {
                records::empty_index(value)?;
                if self
                    .ready
                    .insert((scope, sequence), Some(session))
                    .is_some()
                {
                    return Err(InconsistentIndex);
                }
            }
            Key::Expiry(scope, deadline, sequence) => {
                records::empty_index(value)?;
                if self.expiry.insert((scope, sequence), deadline).is_some() {
                    return Err(InconsistentIndex);
                }
            }
            Key::History(scope, id) => {
                self.history.insert((scope, id), records::decode(value)?);
            }
            Key::HistoryExpiry(scope, deadline, id) => {
                records::empty_index(value)?;
                if self.history_expiry.insert((scope, id), deadline).is_some() {
                    return Err(InconsistentHistory);
                }
            }
        }
        Ok(())
    }
}
