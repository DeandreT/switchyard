use domain::CommittedEntryId;

use crate::LogQueueRefusal;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueWriteResult {
    pub entry: CommittedEntryId,
    pub outcome: QueueWriteOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueWriteOutcome {
    QueueCreated,
    Sent { sequence: u64 },
    Refused(LogQueueRefusal),
}

/// Known rejection concerns this unsubmitted intent, not all replica state.
/// Unknown never implies rollback or permits automatic retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum QueueWriteError {
    #[error("the queue intent was rejected before submission: {0}")]
    KnownRejected(QueueWriteRejection),
    #[error("the submitted queue intent has an unknown result: {0}")]
    Unknown(QueueWriteUnknown),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum QueueWriteRejection {
    #[error("invalid or oversized intent")]
    InvalidIntent,
    #[error("client admission capacity exhausted")]
    Capacity,
    #[error("node admission closed")]
    Closed,
    #[error("node is not the leader")]
    NotLeader,
    #[error("a quorum could not be confirmed")]
    QuorumUnavailable,
    #[error("runtime UTC or committed time exceeds the bounded clock policy")]
    Clock,
    #[error("finite log headroom exhausted")]
    Headroom,
    #[error("healthy metadata unavailable")]
    Storage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum QueueWriteUnknown {
    #[error("leadership changed after submission")]
    LeadershipChanged,
    #[error("node stopped after submission")]
    Stopped,
    #[error("storage failed after submission")]
    Storage,
    #[error("native response unavailable")]
    ResponseUnavailable,
    #[error("original replay result unavailable")]
    OriginalResultUnavailable,
    #[error("incompatible application response")]
    UnexpectedApplication,
    #[error("client owner failed after submission")]
    OwnerLost,
}
