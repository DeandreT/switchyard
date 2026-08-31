use serde::{Deserialize, Serialize};

use crate::{
    AcceptedSession, Delivery, EntityPath, LockToken, MessageEnvelope, NamespaceName, QueueConfig,
    ReceiveMode, SequenceNumber, SessionHold, SessionId, Timestamp,
};

/// One message supplied to an atomic broker operation.
///
/// This is the reusable form of the fields on [`CommandKind::Send`]. Keeping
/// it independent of the command lets protocol adapters build a collection
/// without changing the singular send contract.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessageInput {
    pub message_id: String,
    pub body: Vec<u8>,
    /// Overrides the queue default when set.
    pub time_to_live_millis: Option<u64>,
    /// Required on a queue that requires sessions, and refused on one that
    /// does not.
    pub session_id: Option<SessionId>,
    /// Lossless protocol-native message bytes. The broker keeps these opaque
    /// while using the normalized fields above for its decisions.
    pub envelope: Option<MessageEnvelope>,
}

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
        /// Required on a queue that requires sessions, and refused on one that
        /// does not.
        session_id: Option<SessionId>,
        /// Lossless protocol-native message bytes. The broker keeps these
        /// opaque while using the normalized fields above for its decisions.
        envelope: Option<MessageEnvelope>,
    },
    /// Persists every message in one atomic storage commit.
    SendBatch {
        messages: Vec<MessageInput>,
    },
    Receive {
        mode: ReceiveMode,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
        /// The session lock this receive draws from. Required on a queue that
        /// requires sessions, and refused on one that does not.
        session: Option<SessionHold>,
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
    /// Extends a message lock without changing its token.
    RenewLock {
        sequence: SequenceNumber,
        lock_token: LockToken,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
    },
    /// Takes exclusive ownership of a session.
    AcceptSession {
        /// `None` accepts the next session that has a ready message and is not
        /// already held.
        session_id: Option<SessionId>,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
    },
    /// Gives up a session so another receiver can take it. Messages already
    /// locked inside the session keep their own locks.
    ReleaseSession {
        session: SessionHold,
    },
    /// Extends a session lock without changing its token.
    RenewSessionLock {
        session: SessionHold,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
    },
    /// Replaces the opaque state stored alongside a session.
    SetSessionState {
        session: SessionHold,
        state: Vec<u8>,
    },
    /// Reads the opaque state stored alongside a session.
    GetSessionState {
        session: SessionHold,
    },
    /// Proposed by the leader's timer worker. Returns messages whose lock has
    /// elapsed, or dead-letters them once they reach the delivery limit.
    ExpireLocks,
    /// Proposed by the leader's timer worker. Dead-letters messages whose time
    /// to live has elapsed.
    ExpireMessages,
    /// Proposed by the leader's timer worker. Releases sessions whose lock has
    /// elapsed.
    ExpireSessionLocks,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandOutcome {
    QueueCreated,
    Sent {
        sequence: SequenceNumber,
    },
    BatchSent {
        sequences: Vec<SequenceNumber>,
    },
    /// `None` when the queue held no deliverable message.
    Received(Option<Delivery>),
    Completed,
    Abandoned {
        dead_lettered: bool,
    },
    DeadLettered,
    LockRenewed {
        locked_until: Timestamp,
    },
    LocksExpired {
        returned_to_ready: u32,
        dead_lettered: u32,
    },
    MessagesExpired {
        dead_lettered: u32,
    },
    /// `None` when no session was available to accept.
    SessionAccepted(Option<AcceptedSession>),
    SessionReleased,
    SessionLockRenewed {
        locked_until: Timestamp,
    },
    SessionStateSet,
    SessionState(Vec<u8>),
    SessionLocksExpired {
        released: u32,
    },
}
