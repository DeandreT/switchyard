use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    AcceptedSession, Delivery, EntityPath, LockToken, MessageEnvelope, MessageValue, NamespaceName,
    QueueConfig, QueueConfigUpdate, ReceiveMode, SequenceNumber, SessionHold, SessionId,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig,
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

/// One message to enqueue at a future time. A batch receives its cancellation
/// handles atomically, without becoming visible to ordinary receivers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScheduledMessage {
    pub message_id: String,
    pub body: Vec<u8>,
    /// Requests a lifetime, capped by the queue default when it is finite.
    pub time_to_live_millis: Option<u64>,
    pub session_id: Option<SessionId>,
    pub enqueue_at: Timestamp,
}

/// A scheduled message whose typed AMQP content is retained separately from
/// the compatibility byte-body view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScheduledEnvelope {
    pub message_id: String,
    pub body: Vec<u8>,
    /// Requests a lifetime, capped by the queue default when it is finite.
    pub time_to_live_millis: Option<u64>,
    pub session_id: Option<SessionId>,
    pub enqueue_at: Timestamp,
    pub envelope: MessageEnvelope,
}

/// One independently described message in an atomic ingress batch. An absent
/// scheduled timestamp is an ordinary send, not an immediate schedule.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IngressEnvelope {
    pub message_id: String,
    pub body: Vec<u8>,
    pub time_to_live_millis: Option<u64>,
    pub session_id: Option<SessionId>,
    pub envelope: MessageEnvelope,
    pub scheduled_enqueue_time: Option<Timestamp>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngressBatchLimit {
    Messages,
    ContentBytes,
    ValueItems,
}

impl std::fmt::Display for IngressBatchLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Messages => "message count",
            Self::ContentBytes => "retained content bytes",
            Self::ValueItems => "message value items",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementDisposition {
    Complete,
    Abandon,
    Defer,
    DeadLetter { reason: String, description: String },
}

/// Conservative content budget for a batch delivery. The protocol edge
/// reserves its response wrapper separately and supplies each entry's overhead.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeliveryBudget {
    pub max_bytes: u64,
    pub per_message_overhead_bytes: u64,
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
        /// Requests a lifetime, capped by the queue default when it is finite.
        time_to_live_millis: Option<u64>,
        /// Required on a queue that requires sessions, and refused on one that
        /// does not.
        session_id: Option<SessionId>,
    },
    Schedule {
        messages: Vec<ScheduledMessage>,
    },
    /// Removes scheduled messages before activation. Every sequence must
    /// still identify a scheduled message, otherwise the batch changes nothing.
    CancelScheduled {
        sequences: Vec<SequenceNumber>,
    },
    Receive {
        mode: ReceiveMode,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
        /// The session lock this receive draws from. Required on a queue that
        /// requires sessions, and refused on one that does not.
        session: Option<SessionHold>,
    },
    /// Browses messages without changing their state.
    Peek {
        /// First sequence number to inspect, inclusive.
        from_sequence: SequenceNumber,
        max_messages: u32,
        /// Narrows the browse to one session. Required on a session queue, and
        /// refused on a non-session queue.
        session_id: Option<SessionId>,
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
    Defer {
        sequence: SequenceNumber,
        lock_token: LockToken,
    },
    /// Extends a message lock without changing its token.
    RenewLock {
        sequence: SequenceNumber,
        lock_token: LockToken,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
    },
    ReceiveDeferred {
        sequences: Vec<SequenceNumber>,
        mode: ReceiveMode,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
        /// Narrows deferred receive to one session. Required on a session
        /// queue, and refused on a non-session queue.
        session_id: Option<SessionId>,
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
    /// Proposed by the leader's timer worker. Expires ready messages whose time
    /// to live has elapsed. Live locks and deferred messages are not swept.
    ExpireMessages,
    /// Proposed by the leader's timer worker. Releases sessions whose lock has
    /// elapsed.
    ExpireSessionLocks,
    /// Proposed by the leader's timer worker. Enqueues scheduled messages
    /// whose requested enqueue time has arrived.
    ActivateScheduled,
    /// Proposed by the leader's timer worker. Discards message identifiers
    /// whose duplicate-detection history window has elapsed.
    ExpireDuplicateHistory,
    SendEnvelope {
        message_id: String,
        body: Vec<u8>,
        /// Requests a lifetime, capped by the queue default when it is finite.
        time_to_live_millis: Option<u64>,
        session_id: Option<SessionId>,
        envelope: Box<MessageEnvelope>,
    },
    ScheduleEnvelopes {
        messages: Vec<ScheduledEnvelope>,
    },
    /// Settles one delivery while atomically replacing the supplied
    /// application properties. Unmentioned properties are retained.
    Settle {
        sequence: SequenceNumber,
        lock_token: LockToken,
        disposition: SettlementDisposition,
        properties_to_modify: BTreeMap<String, MessageValue>,
    },
    /// Retrieves an atomic batch only when all inspected messages, including
    /// expired messages cleaned up by the receive, fit the delivery budget.
    ReceiveDeferredBounded {
        sequences: Vec<SequenceNumber>,
        mode: ReceiveMode,
        lock_duration_millis: Option<u64>,
        session_id: Option<SessionId>,
        budget: DeliveryBudget,
    },
    /// Browses a fitting prefix while reading at most one stored message at
    /// a time. An oversized first result is rejected rather than omitted.
    PeekBounded {
        from_sequence: SequenceNumber,
        max_messages: u32,
        session_id: Option<SessionId>,
        budget: DeliveryBudget,
    },
    /// Retrieves a bounded batch while proving live session ownership before
    /// inspecting or expiring any message. A non-session queue requires None.
    ReceiveDeferredHeld {
        sequences: Vec<SequenceNumber>,
        mode: ReceiveMode,
        lock_duration_millis: Option<u64>,
        session: Option<SessionHold>,
        budget: DeliveryBudget,
    },
    /// Replaces only the supplied mutable settings. Existing message and lock
    /// deadlines, counters, and duplicate-history entries are retained.
    UpdateQueue {
        update: QueueConfigUpdate,
    },
    /// Enqueues every member atomically after validating the whole bounded
    /// batch. Duplicate drops still consume their own acknowledged sequences.
    SendBatch {
        messages: Vec<IngressEnvelope>,
    },
    /// Creates a topic at Command.entity.
    CreateTopic {
        config: TopicConfig,
    },
    /// Creates one subscription under the parent topic named by Command.entity.
    CreateSubscription {
        name: SubscriptionName,
        config: SubscriptionConfig,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandOutcome {
    QueueCreated,
    Sent {
        sequence: SequenceNumber,
    },
    Scheduled {
        sequences: Vec<SequenceNumber>,
    },
    ScheduledCancelled {
        cancelled: u32,
    },
    ScheduledActivated {
        activated: u32,
    },
    DuplicateHistoryExpired {
        expired: u32,
    },
    /// `None` when the queue held no deliverable message.
    Received(Option<Delivery>),
    Peeked(Vec<Delivery>),
    Completed,
    Abandoned {
        dead_lettered: bool,
        dropped: bool,
    },
    DeadLettered,
    Deferred,
    LockRenewed {
        locked_until: Timestamp,
    },
    DeferredReceived(Vec<Delivery>),
    LocksExpired {
        returned_to_ready: u32,
        dead_lettered: u32,
        dropped: u32,
    },
    MessagesExpired {
        dead_lettered: u32,
        dropped: u32,
        /// Due index entries consumed, including repairs of legacy entries.
        processed: u32,
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
    QueueUpdated,
    BatchSent {
        sequences: Vec<SequenceNumber>,
    },
    TopicCreated,
    SubscriptionCreated,
}
