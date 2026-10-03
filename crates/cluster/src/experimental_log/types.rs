use std::fmt;

use domain::{
    CommittedQueueCommand, CommittedQueueWork, CommittedSend, CommittedStreamId, EntityPath,
    NamespaceName, QueueConfig, Timestamp,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const MAX_LOG_BODY_BYTES: usize = domain::MAX_COMMITTED_BODY_BYTES;
pub const MAX_LOG_ENTRY_BYTES: usize = domain::MAX_COMMITTED_ENTRY_BYTES;
pub const MAX_LOG_QUEUE_BYTES: usize = MAX_LOG_ENTRY_BYTES - 128;
pub const MAX_LOG_MEMBERSHIP_BYTES: usize = domain::MAX_COMMITTED_MEMBERSHIP_BYTES;
pub const MAX_LOG_METADATA_BYTES: usize = 8 * 1024;
pub const MAX_APPEND_ENTRIES: usize = 32;
pub const MAX_APPEND_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_RETAINED_ENTRIES: u64 = 256;
pub const MAX_RETAINED_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_LIMITED_ENTRIES: usize = 32;
pub const MAX_LIMITED_BYTES: usize = 4 * 1024 * 1024;

pub(super) const PROFILE_KEY: &[u8] = &[1];
pub(super) const PROGRESS_KEY: &[u8] = &[2];
pub(super) const ENTRY_PREFIX: u8 = 0x10;

pub(super) fn entry_key(index: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(ENTRY_PREFIX);
    key.extend_from_slice(&index.to_be_bytes());
    key
}

openraft::declare_raft_types!(
    pub LogTypes:
        D = QueueLogCommand,
        R = crate::experimental_state_machine::LogApplication,
        NodeId = u64,
        Node = openraft::BasicNode,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type LogEntry = openraft::Entry<LogTypes>;
pub type LogId = openraft::LogId<u64>;
pub type LogVote = openraft::Vote<u64>;

/// Restricted, leader-stamped queue data. This is not an RPC or apply API.
#[derive(Clone, Eq, PartialEq)]
pub struct QueueLogCommand(pub(super) Box<QueueLogKind>);

#[derive(Clone, Eq, PartialEq)]
pub(super) enum QueueLogKind {
    CreateQueue {
        namespace: NamespaceName,
        entity: EntityPath,
        issued_at: Timestamp,
        config: QueueConfig,
    },
    Send {
        namespace: NamespaceName,
        entity: EntityPath,
        issued_at: Timestamp,
        message: CommittedSend,
    },
}

impl QueueLogCommand {
    pub fn create_queue(
        namespace: NamespaceName,
        entity: EntityPath,
        issued_at: Timestamp,
        config: QueueConfig,
    ) -> Self {
        Self(Box::new(QueueLogKind::CreateQueue {
            namespace,
            entity,
            issued_at,
            config,
        }))
    }

    pub fn send(
        namespace: NamespaceName,
        entity: EntityPath,
        issued_at: Timestamp,
        message: CommittedSend,
    ) -> Self {
        Self(Box::new(QueueLogKind::Send {
            namespace,
            entity,
            issued_at,
            message,
        }))
    }

    pub fn into_committed_work(self) -> CommittedQueueWork {
        let command = match *self.0 {
            QueueLogKind::CreateQueue {
                namespace,
                entity,
                issued_at,
                config,
            } => CommittedQueueCommand::create_queue(namespace, entity, issued_at, config),
            QueueLogKind::Send {
                namespace,
                entity,
                issued_at,
                message,
            } => CommittedQueueCommand::send(namespace, entity, issued_at, message),
        };
        CommittedQueueWork::Queue(command)
    }
}

impl fmt::Debug for QueueLogCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueueLogCommand")
            .field("is_send", &matches!(*self.0, QueueLogKind::Send { .. }))
            .finish_non_exhaustive()
    }
}

impl Serialize for QueueLogCommand {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        super::codec::serialize_queue(self, serializer)
    }
}

impl<'de> Deserialize<'de> for QueueLogCommand {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        super::codec::deserialize_queue(deserializer)
    }
}

/// Immutable identity for a separate experimental log directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogProfile {
    node_id: u64,
    stream: CommittedStreamId,
}

impl LogProfile {
    pub fn new(node_id: u64, stream: CommittedStreamId) -> Result<Self, LogCodecError> {
        let profile = Self { node_id, stream };
        profile.validate()?;
        Ok(profile)
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn stream(&self) -> CommittedStreamId {
        self.stream
    }

    pub(super) fn validate(&self) -> Result<(), LogCodecError> {
        CommittedStreamId::new(*self.stream.as_bytes())
            .map(|_| ())
            .map_err(|_| LogCodecError::InvalidProfile)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct LogProgress {
    pub(super) vote: Option<LogVote>,
    pub(super) last_purged: Option<LogId>,
    pub(super) last_present: Option<LogId>,
    pub(super) retained_entries: u64,
    pub(super) retained_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogResource {
    Body,
    Entry,
    Membership,
    Metadata,
    AppendEntries,
    AppendBytes,
}

impl fmt::Display for LogResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LogCodecError {
    #[error("the experimental log profile is invalid")]
    InvalidProfile,
    #[error("the experimental log progress is inconsistent")]
    InvalidProgress,
    #[error("the log entry contains an invalid structural identifier")]
    InvalidIdentifier,
    #[error("the log membership is invalid or exceeds its structural bounds")]
    InvalidMembership,
    #[error("the log record header or version is unsupported")]
    UnsupportedRecord,
    #[error("the log record is malformed")]
    Malformed,
    #[error("the log record is not canonically encoded")]
    NonCanonical,
    #[error("log {resource} exceeds its {maximum}-byte or item bound")]
    TooLarge {
        resource: LogResource,
        maximum: usize,
    },
    #[error("the log size cannot be represented")]
    SizeOverflow,
}

pub(super) struct EncodedEntry {
    log_id: LogId,
    bytes: Vec<u8>,
}

impl EncodedEntry {
    pub(super) fn validated(log_id: LogId, bytes: Vec<u8>) -> Self {
        Self { log_id, bytes }
    }

    pub(super) fn id(&self) -> LogId {
        self.log_id
    }

    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn encoded_len(&self) -> usize {
        self.bytes.len()
    }
}

impl fmt::Debug for EncodedEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedEntry")
            .field("log_id", &self.log_id)
            .field("encoded_bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

pub(super) struct EncodedAppend {
    entries: Vec<EncodedEntry>,
    bytes: usize,
}

impl EncodedAppend {
    pub(super) fn from_entries<I: IntoIterator<Item = LogEntry>>(
        entries: I,
    ) -> Result<Self, LogCodecError> {
        let mut packet = Self {
            entries: Vec::new(),
            bytes: 0,
        };
        for entry in entries {
            if packet.entries.len() == MAX_APPEND_ENTRIES {
                return Err(LogCodecError::TooLarge {
                    resource: LogResource::AppendEntries,
                    maximum: MAX_APPEND_ENTRIES,
                });
            }
            let encoded = super::codec::encode_entry(&entry)?;
            packet.bytes = packet
                .bytes
                .checked_add(encoded.encoded_len())
                .ok_or(LogCodecError::SizeOverflow)?;
            if packet.bytes > MAX_APPEND_BYTES {
                return Err(LogCodecError::TooLarge {
                    resource: LogResource::AppendBytes,
                    maximum: MAX_APPEND_BYTES,
                });
            }
            packet.entries.push(encoded);
        }
        Ok(packet)
    }

    pub(super) fn entries(&self) -> &[EncodedEntry] {
        &self.entries
    }

    pub(super) fn encoded_bytes(&self) -> usize {
        self.bytes
    }
}
