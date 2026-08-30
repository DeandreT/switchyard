//! Broker identifiers, commands, state-machine rules, and errors.
//!
//! This crate holds the deterministic core of Switchyard. It knows nothing
//! about networking, consensus, or the AMQP wire format: it turns a replicated
//! [`Command`] into an atomic batch of storage mutations. Consensus decides the
//! order commands are applied in; this crate decides what each one means.

#![forbid(unsafe_code)]

pub mod codec;
pub mod keys;

mod command;
mod error;
mod identifier;
mod machine;
mod message;
mod queue;
mod session;
mod time;

pub use codec::CodecError;
pub use command::{Command, CommandKind, CommandOutcome};
pub use error::BrokerError;
pub use identifier::{
    DEAD_LETTER_QUEUE_SUFFIX, EntityPath, IdentifierError, MAX_ENTITY_PATH_BYTES,
    MAX_NAMESPACE_NAME_BYTES, MAX_PLACEMENT_GROUP_ID_BYTES, MAX_SESSION_ID_BYTES, NamespaceName,
    PlacementGroupId, SessionId,
};
pub use machine::{StateMachine, TIMER_SCAN_LIMIT};
pub use message::{
    DeadLetterInfo, DeadLetterReason, Delivery, DeliveryGuarantee, DeliveryLock, LockToken,
    MessageEnvelope, MessageRecord, MessageState, ReceiveMode, SequenceNumber,
};
pub use queue::{
    DEFAULT_LOCK_DURATION_MILLIS, DEFAULT_MAX_DELIVERY_COUNT, DEFAULT_MAX_MESSAGE_BYTES,
    MAX_LOCK_DURATION_MILLIS, QueueConfig, QueueConfigError, QueueCounters,
};
pub use session::{AcceptedSession, SessionHold, SessionLock, SessionRecord};
pub use time::Timestamp;
