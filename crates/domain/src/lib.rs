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
mod message_content;
mod queue;
mod session;
mod time;
mod topic;

pub use codec::CodecError;
pub use command::{
    Command, CommandKind, CommandOutcome, DeliveryBudget, IngressBatchLimit, IngressEnvelope,
    ScheduledEnvelope, ScheduledMessage, SettlementDisposition,
};
pub use error::BrokerError;
pub use identifier::{
    DEAD_LETTER_QUEUE_SUFFIX, EntityPath, IdentifierError, MAX_ENTITY_PATH_BYTES,
    MAX_NAMESPACE_NAME_BYTES, MAX_PLACEMENT_GROUP_ID_BYTES, MAX_SESSION_ID_BYTES,
    MAX_SUBSCRIPTION_NAME_BYTES, NamespaceName, PlacementGroupId, SUBSCRIPTION_PATH_SEGMENT,
    SessionId, SubscriptionName,
};
pub use machine::{
    BROKER_HEADER_RESERVE_BYTES, CommandApplication, MAX_DEAD_LETTER_DETAIL_LENGTH,
    MAX_INGRESS_BATCH_CONTENT_BYTES, MAX_INGRESS_BATCH_MESSAGES, MAX_INGRESS_BATCH_VALUE_ITEMS,
    MAX_QUEUE_PAGE_SIZE, MAX_TOPIC_FANOUT_CONTENT_BYTES, MAX_TOPIC_FANOUT_COPIES,
    MAX_TOPIC_FANOUT_VALUE_ITEMS, MAX_TOPIC_PAGE_SIZE, QueueCursor, QueuePage, StateMachine,
    TIMER_SCAN_LIMIT, TopicCursor, TopicPage,
};
pub use message::{
    DeadLetterInfo, DeadLetterReason, Delivery, DeliveryGuarantee, DeliveryLock, LockToken,
    MAX_MESSAGE_ID_LENGTH, MessageRecord, MessageState, MessageStatus, ReceiveMode, SequenceNumber,
};
pub use message_content::{
    AnnotationKey, MAX_MESSAGE_HEADER_BYTES, MAX_MESSAGE_PROPERTY_BYTES, MAX_MESSAGE_VALUE_DEPTH,
    MAX_MESSAGE_VALUE_ITEMS, MessageBody, MessageDescriptor, MessageEnvelope, MessageHeader,
    MessageIdentifier, MessageProperties, MessageValue,
};
pub use queue::{
    DEFAULT_DUPLICATE_DETECTION_WINDOW_MILLIS, DEFAULT_LOCK_DURATION_MILLIS,
    DEFAULT_MAX_DELIVERY_COUNT, DEFAULT_MAX_MESSAGE_BYTES, MAX_DUPLICATE_DETECTION_WINDOW_MILLIS,
    MAX_LOCK_DURATION_MILLIS, MAX_SEQUENCE_NUMBER, MIN_DUPLICATE_DETECTION_WINDOW_MILLIS,
    QueueConfig, QueueConfigError, QueueConfigUpdate, QueueCounterKind, QueueCounters,
    QueueImmutableProperty, QueueTimeToLiveUpdate,
};
pub use session::{AcceptedSession, SessionHold, SessionLock, SessionRecord};
pub use time::Timestamp;
pub use topic::{MAX_TOPIC_SUBSCRIPTIONS, SubscriptionConfig, SubscriptionDefinition, TopicConfig};
