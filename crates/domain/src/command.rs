use serde::{Deserialize, Serialize};

use crate::{
    Delivery, EntityPath, LockToken, NamespaceName, QueueConfig, ReceiveMode, SequenceNumber,
    Timestamp,
};

/// One replicated instruction for the broker state machine.
///
/// The leader stamps `issued_at` before proposing. Followers apply the same
/// value, which is what keeps lock deadlines and expiry decisions identical
/// across replicas.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub namespace: NamespaceName,
    pub entity: EntityPath,
    pub issued_at: Timestamp,
    pub kind: CommandKind,
}

impl Command {
    pub fn new(
        namespace: NamespaceName,
        entity: EntityPath,
        issued_at: Timestamp,
        kind: CommandKind,
    ) -> Self {
        Self {
            namespace,
            entity,
            issued_at,
            kind,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandKind {
    CreateQueue {
        config: QueueConfig,
    },
    Send {
        message_id: String,
        body: Vec<u8>,
        /// Overrides the queue default when set.
        time_to_live_millis: Option<u64>,
    },
    Receive {
        mode: ReceiveMode,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
    },
    Complete {
        sequence: SequenceNumber,
        lock_token: LockToken,
    },
    Abandon {
        sequence: SequenceNumber,
        lock_token: LockToken,
    },
    DeadLetter {
        sequence: SequenceNumber,
        lock_token: LockToken,
        reason: String,
        description: String,
    },
    /// Proposed by the leader's timer worker. Returns messages whose lock has
    /// elapsed, or dead-letters them once they reach the delivery limit.
    ExpireLocks,
    /// Proposed by the leader's timer worker. Dead-letters messages whose time
    /// to live has elapsed.
    ExpireMessages,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandOutcome {
    QueueCreated,
    Sent {
        sequence: SequenceNumber,
    },
    /// `None` when the queue held no deliverable message.
    Received(Option<Delivery>),
    Completed,
    Abandoned {
        dead_lettered: bool,
    },
    DeadLettered,
    LocksExpired {
        returned_to_ready: u32,
        dead_lettered: u32,
    },
    MessagesExpired {
        dead_lettered: u32,
    },
}
