//! AMQP 1.0 and Azure Service Bus protocol adaptation.
//!
//! Wire concerns only: what a client attached to, what a rejection is called on
//! the wire, and what a message may weigh. Delivery semantics belong to the
//! domain crate and are re-exported rather than restated.

#![forbid(unsafe_code)]

mod address;
mod authorization;
mod broker;
mod cbs;
mod condition;
mod frame_adapter;
mod listener;
mod message;
mod session_filter;
mod tls;

use thiserror::Error;

pub use crate::{
    address::{
        Attachment, DEAD_LETTER_SUFFIX, SUBSCRIPTION_SEGMENT, namespace_from_hostname,
        parse_attachment, parse_session_id,
    },
    authorization::SharedAccessAuthentication,
    broker::{Broker, BrokerRejection},
    condition::{
        ENTITY_ALREADY_EXISTS, INTERNAL_ERROR, MESSAGE_LOCK_LOST, MESSAGE_SIZE_EXCEEDED,
        NOT_ALLOWED, NOT_FOUND, PRECONDITION_FAILED, RESOURCE_LOCKED, SESSION_CANNOT_BE_LOCKED,
        SESSION_LOCK_LOST, TIMEOUT, condition_for, is_retryable,
    },
    listener::AmqpListener,
    message::{
        DEAD_LETTER_DESCRIPTION_PROPERTY, DEAD_LETTER_REASON_PROPERTY, IncomingMessage,
        read_incoming, write_delivery,
    },
    session_filter::{SESSION_FILTER, SessionRequest, read_session_filter, stamp_session_filter},
    tls::{TlsConfigurationError, tls_server_config},
};

/// Settlement modes are broker semantics, not wire syntax, so they live in the
/// domain crate. The protocol edge maps AMQP receiver settle modes onto them.
pub use domain::{DeliveryGuarantee, ReceiveMode};

pub const AMQP_TLS_PORT: u16 = 5671;
pub const AMQP_WEBSOCKET_PORT: u16 = 443;
pub const CBS_NODE: &str = "$cbs";
pub const MANAGEMENT_NODE: &str = "$management";

/// Wire limit the edge rejects against before a command reaches the broker. A
/// queue may be configured with a smaller limit, which the state machine
/// enforces on its own.
pub const SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES: usize = domain::DEFAULT_MAX_MESSAGE_BYTES;

pub fn validate_standard_message_size(encoded_bytes: usize) -> Result<(), ProtocolError> {
    if encoded_bytes > SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES {
        return Err(ProtocolError::MessageTooLarge {
            encoded_bytes,
            maximum_bytes: SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES,
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProtocolError {
    #[error("encoded message size {encoded_bytes} exceeds the {maximum_bytes}-byte limit")]
    MessageTooLarge {
        encoded_bytes: usize,
        maximum_bytes: usize,
    },
    #[error("the connection named no namespace")]
    MissingNamespace,
    #[error("address {address:?} does not name an entity: {detail}")]
    InvalidAddress { address: String, detail: String },
    #[error("session id {session_id:?} is not usable: {detail}")]
    InvalidSessionId { session_id: String, detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_message_limit_is_inclusive() {
        assert_eq!(
            validate_standard_message_size(SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES),
            Ok(())
        );
        assert!(
            validate_standard_message_size(SERVICE_BUS_STANDARD_MAX_MESSAGE_BYTES + 1).is_err()
        );
    }
}
