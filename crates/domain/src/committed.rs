//! Bounded, typed committed-entry application, independent of a consensus engine.

use std::fmt;

use serde::{Deserialize, Serialize};
use storage::StorageError;

use crate::{
    BrokerError, Command, CommandApplication, CommandKind, EntityPath, IdentifierError,
    NamespaceName, QueueConfig, SessionId, Timestamp,
};

mod checkpoint;
mod fingerprint;

#[cfg(test)]
mod tests;

pub(crate) use checkpoint::{decode_checkpoint, encode_checkpoint};
pub(crate) use fingerprint::entry_fingerprint;

pub const MAX_COMMITTED_BODY_BYTES: usize = 256 * 1024;
pub const MAX_COMMITTED_ENTRY_BYTES: usize = MAX_COMMITTED_BODY_BYTES + 4 * 1024;
pub const MAX_COMMITTED_MEMBERSHIP_BYTES: usize = 4 * 1024;
pub const MAX_COMMITTED_CHECKPOINT_BYTES: usize = 8 * 1024;

/// Explicit stream identity. This is not authentication or cluster membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommittedStreamId([u8; 16]);

impl CommittedStreamId {
    pub fn new(bytes: [u8; 16]) -> Result<Self, CommittedApplyError> {
        if bytes == [0; 16] {
            return Err(CommittedApplyError::InvalidStreamId);
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub(crate) fn validate(self) -> Result<(), CommittedApplyError> {
        Self::new(self.0).map(|_| ())
    }
}

/// Full committed log identity, including the leader identity, not only index.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommittedEntryId {
    pub term: u64,
    pub node_id: u64,
    pub index: u64,
}

/// Content identity is derived internally from a frozen canonical entry schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommittedEntryMark {
    pub id: CommittedEntryId,
    pub fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedCheckpointUpdate {
    pub stream: CommittedStreamId,
    pub expected_previous: Option<CommittedEntryMark>,
    pub entry: CommittedEntryId,
}

/// The consensus adapter owns interpretation and validation of these bytes.
#[derive(Clone, Eq, PartialEq)]
pub struct CommittedMembership {
    pub source: CommittedEntryId,
    pub schema_version: u16,
    pub payload: Vec<u8>,
}

impl fmt::Debug for CommittedMembership {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommittedMembership")
            .field("source", &self.source)
            .field("schema_version", &self.schema_version)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedCheckpoint {
    pub(crate) stream: CommittedStreamId,
    pub(crate) last: Option<CommittedEntryMark>,
    pub(crate) previous: Option<CommittedEntryMark>,
    pub(crate) highest_timestamp: Timestamp,
    pub(crate) membership: Option<CommittedMembership>,
}

impl CommittedCheckpoint {
    pub fn stream(&self) -> CommittedStreamId {
        self.stream
    }

    pub fn last(&self) -> Option<CommittedEntryMark> {
        self.last
    }

    pub fn previous(&self) -> Option<CommittedEntryMark> {
        self.previous
    }

    pub fn highest_timestamp(&self) -> Timestamp {
        self.highest_timestamp
    }

    pub fn membership(&self) -> Option<&CommittedMembership> {
        self.membership.as_ref()
    }

    pub(crate) fn initial(stream: CommittedStreamId) -> Self {
        Self {
            stream,
            last: None,
            previous: None,
            highest_timestamp: Timestamp::UNIX_EPOCH,
            membership: None,
        }
    }
}

/// Owned legacy byte-body ingress. No typed envelope, scheduling, or settlement.
#[derive(Clone, Eq, PartialEq)]
pub struct CommittedSend {
    pub message_id: String,
    pub body: Vec<u8>,
    pub time_to_live_millis: Option<u64>,
    pub session_id: Option<SessionId>,
}

impl fmt::Debug for CommittedSend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommittedSend")
            .field("message_id_bytes", &self.message_id.len())
            .field("body_bytes", &self.body.len())
            .field("time_to_live_millis", &self.time_to_live_millis)
            .field("has_session", &self.session_id.is_some())
            .finish()
    }
}

/// Restricted constructors make arbitrary domain command kinds unavailable.
///
/// ```compile_fail
/// fn unrestricted(command: domain::Command) -> domain::CommittedQueueCommand {
///     command.into()
/// }
/// ```
#[derive(Clone, Eq, PartialEq)]
pub struct CommittedQueueCommand(Box<Command>);

impl CommittedQueueCommand {
    pub fn create_queue(
        namespace: NamespaceName,
        entity: EntityPath,
        issued_at: Timestamp,
        config: QueueConfig,
    ) -> Self {
        Self(Box::new(Command::new(
            namespace,
            entity,
            issued_at,
            CommandKind::CreateQueue { config },
        )))
    }

    pub fn send(
        namespace: NamespaceName,
        entity: EntityPath,
        issued_at: Timestamp,
        message: CommittedSend,
    ) -> Self {
        Self(Box::new(Command::new(
            namespace,
            entity,
            issued_at,
            CommandKind::Send {
                message_id: message.message_id,
                body: message.body,
                time_to_live_millis: message.time_to_live_millis,
                session_id: message.session_id,
            },
        )))
    }

    pub(crate) fn as_command(&self) -> &Command {
        &self.0
    }
}

impl fmt::Debug for CommittedQueueCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommittedQueueCommand")
            .field("issued_at", &self.0.issued_at)
            .field("is_send", &matches!(self.0.kind, CommandKind::Send { .. }))
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum CommittedQueueWork {
    Blank,
    Membership {
        schema_version: u16,
        payload: Vec<u8>,
    },
    Queue(CommittedQueueCommand),
}

impl CommittedQueueWork {
    /// Computes the bounded canonical mark for this work and its predecessor.
    ///
    /// This pure calculation performs no storage or clock access. The mark is
    /// not evidence of commitment, application, authority, or a business outcome.
    /// Hashable business refusals remain representable.
    pub fn entry_mark(
        &self,
        update: &CommittedCheckpointUpdate,
    ) -> Result<CommittedEntryMark, CommittedApplyError> {
        Ok(CommittedEntryMark {
            id: update.entry,
            fingerprint: entry_fingerprint(update, self)?,
        })
    }
}

impl fmt::Debug for CommittedQueueWork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Blank => formatter.write_str("Blank"),
            Self::Membership {
                schema_version,
                payload,
            } => formatter
                .debug_struct("Membership")
                .field("schema_version", schema_version)
                .field("payload_bytes", &payload.len())
                .finish(),
            Self::Queue(command) => command.fmt(formatter),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommittedApplication {
    CheckpointOnly,
    Queue(Box<CommandApplication>),
    Refused(BrokerError),
}

/// Replay deliberately carries no reconstructed result or success effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommittedApplyResult {
    Applied {
        position: CommittedEntryMark,
        application: CommittedApplication,
    },
    AlreadyApplied {
        position: CommittedEntryMark,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CommittedApplyError {
    #[error("committed stream identity must not be zero")]
    InvalidStreamId,
    #[error("the replica has not been initialized with a committed checkpoint")]
    NotInitialized,
    #[error("replica initialization requires an uninitialized empty store")]
    NotPristine,
    #[error("the committed checkpoint is missing, corrupt, or unsupported")]
    CorruptCheckpoint,
    #[error("the committed stream does not match this replica")]
    WrongStream,
    #[error("the same committed entry identity carries different work")]
    ReplayConflict,
    #[error("the expected previous entry does not match committed progress")]
    PreviousMismatch,
    #[error("the next committed entry is not contiguous")]
    NonContiguous,
    #[error("the committed entry index is exhausted")]
    IndexExhausted,
    #[error("committed membership requires a nonzero schema version")]
    InvalidMembershipSchema,
    #[error("committed {resource} exceeds its {maximum}-byte bound")]
    TooLarge {
        resource: &'static str,
        maximum: usize,
    },
    #[error("the canonical committed entry could not be encoded")]
    EntryEncoding,
    #[error("the trusted committed entry contains an invalid identifier: {0}")]
    InvalidIdentifier(#[from] IdentifierError),
    #[error("stored business state cannot be safely applied: {0}")]
    BusinessState(BrokerError),
    #[error("business clock {applied} exceeds committed watermark {watermark}")]
    BusinessClockAhead {
        applied: Timestamp,
        watermark: Timestamp,
    },
    #[error("the committed writer must be reopened after an indeterminate write")]
    Poisoned,
    #[error(transparent)]
    Storage(#[from] StorageError),
}
