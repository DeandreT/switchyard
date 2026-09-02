use serde::{Deserialize, Serialize};

use crate::{
    AcceptedSession, Delivery, EntityPath, LockToken, MessageEnvelope, NamespaceName, QueueConfig,
    ReceiveMode, SequenceNumber, SessionHold, SessionId, SubscriptionConfig, SubscriptionName,
    Timestamp, TopicConfig,
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
    /// Keeps the message outside the ready and expiry indexes until this
    /// replicated timestamp is reached by the timer worker.
    pub scheduled_enqueue_at: Option<Timestamp>,
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
    CreateTopic {
        config: TopicConfig,
    },
    /// Creates one durable match-all subscription below `Command::entity`,
    /// which names its already-existing topic.
    CreateSubscription {
        name: SubscriptionName,
        config: SubscriptionConfig,
    },
    Send {
        message_id: String,
        body: Vec<u8>,
        /// Overrides the queue default when set.
        time_to_live_millis: Option<u64>,
        /// Required on a queue that requires sessions, and refused on one that
        /// does not.
        session_id: Option<SessionId>,
        /// When set, persists a browseable scheduled placeholder instead of
        /// making the message immediately available to receivers.
        scheduled_enqueue_at: Option<Timestamp>,
        /// Lossless protocol-native message bytes. The broker keeps these
        /// opaque while using the normalized fields above for its decisions.
        envelope: Option<MessageEnvelope>,
    },
    /// Persists every message in one atomic storage commit.
    SendBatch {
        messages: Vec<MessageInput>,
    },
    /// Atomically removes scheduled placeholders. Every sequence must still
    /// name a scheduled message or the command writes nothing.
    CancelScheduled {
        sequences: Vec<SequenceNumber>,
    },
    /// Browses stored messages without acquiring a lock or changing their
    /// delivery state. `from_sequence` is inclusive; zero starts at the first
    /// available message.
    Peek {
        from_sequence: SequenceNumber,
        max_messages: u32,
        /// `None` browses every session on a session queue. A supplied hold
        /// must still be live and restricts the page to that session.
        session: Option<SessionHold>,
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
        /// An updated protocol envelope after applying properties-to-modify.
        /// `None` keeps the envelope that arrived with the send.
        replacement_envelope: Option<MessageEnvelope>,
    },
    /// Removes a locked message from ordinary delivery until a receiver asks
    /// for its sequence number explicitly.
    Defer {
        sequence: SequenceNumber,
        lock_token: LockToken,
        /// An updated protocol envelope after applying properties-to-modify.
        /// `None` keeps the envelope that arrived with the send.
        replacement_envelope: Option<MessageEnvelope>,
    },
    /// Atomically retrieves explicitly deferred messages in caller order.
    ReceiveDeferred {
        sequences: Vec<SequenceNumber>,
        mode: ReceiveMode,
        /// Overrides the queue default when set.
        lock_duration_millis: Option<u64>,
        /// Required on a queue that requires sessions. Every requested message
        /// must belong to the held session.
        session: Option<SessionHold>,
    },
    DeadLetter {
        sequence: SequenceNumber,
        lock_token: LockToken,
        reason: String,
        description: String,
        /// An updated protocol envelope after applying properties-to-modify.
        /// `None` keeps the envelope that arrived with the send.
        replacement_envelope: Option<MessageEnvelope>,
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
    /// Proposed by the leader's timer worker. Re-enqueues due scheduled
    /// placeholders with new active sequence numbers.
    ActivateScheduled,
    /// Proposed by the leader's timer worker. Removes message identifiers whose
    /// duplicate-detection history window elapsed.
    ExpireDuplicateHistory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandOutcome {
    QueueCreated,
    TopicCreated,
    SubscriptionCreated {
        entity: EntityPath,
    },
    Sent {
        sequence: SequenceNumber,
    },
    /// The send was accepted but its message identifier was still present in
    /// the entity's duplicate-detection history.
    DuplicateSuppressed {
        sequence: SequenceNumber,
    },
    BatchSent {
        sequences: Vec<SequenceNumber>,
        /// Children that were actually persisted; every input still receives a
        /// deterministic sequence slot in `sequences`.
        stored: u32,
    },
    /// Immediate topic fanout committed atomically. Every sequence is owned by
    /// the topic and stamped identically into that message's subscription
    /// copies. An empty subscription list is a successful publish to a topic
    /// that currently has no subscribers.
    Published {
        sequences: Vec<SequenceNumber>,
        subscriptions: Vec<EntityPath>,
    },
    ScheduledCancelled {
        cancelled: u32,
    },
    ScheduledActivated {
        activated: u32,
    },
    DuplicateHistoryExpired {
        removed: u32,
    },
    Peeked(Vec<Delivery>),
    /// `None` when the queue held no deliverable message.
    Received(Option<Delivery>),
    Completed,
    Abandoned {
        dead_lettered: bool,
    },
    Deferred,
    /// Live requested messages, in caller order. An expired deferred message
    /// is moved to the dead-letter queue and omitted.
    DeferredReceived(Vec<Delivery>),
    DeadLettered,
    LockRenewed {
        locked_until: Timestamp,
        /// Effective duration used to extend the lock, after applying any
        /// per-renewal override.
        lock_duration_millis: u64,
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
