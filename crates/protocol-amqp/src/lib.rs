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
mod listener;
mod management;
mod message;
mod session_filter;
mod tls;
mod websocket;

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
    management::{
        ASSOCIATED_LINK_NAME_PROPERTY, ERROR_CONDITION_PROPERTY, EXPIRATION, EXPIRATIONS,
        GET_SESSION_STATE_OPERATION, LOCK_TOKENS, OPERATION_PROPERTY, PEEK_MESSAGE_OPERATION,
        RECEIVE_BY_SEQUENCE_NUMBER_OPERATION, RENEW_LOCK_OPERATION, RENEW_SESSION_LOCK_OPERATION,
        SESSION_ID, SESSION_STATE, SET_SESSION_STATE_OPERATION, STATUS_CODE_PROPERTY,
        STATUS_DESCRIPTION_PROPERTY, TRACKING_ID_PROPERTY, UPDATE_DISPOSITION_OPERATION,
    },
    message::{
        DEAD_LETTER_DESCRIPTION_PROPERTY, DEAD_LETTER_REASON_PROPERTY, IncomingMessage,
        IncomingMessages, SERVICE_BUS_BATCH_MESSAGE_FORMAT, read_incoming, read_incoming_messages,
        write_delivery,
    },
    session_filter::{SESSION_FILTER, SessionRequest, read_session_filter, stamp_session_filter},
    tls::{TlsConfigurationError, tls_server_config},
    websocket::{
        AMQP_WEBSOCKET_STANDARD_SUBPROTOCOL, AMQP_WEBSOCKET_SUBPROTOCOL,
        SERVICE_BUS_WEBSOCKET_PATH, WebSocketIo, accept_amqp_websocket,
    },
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
    #[error("the stored AMQP message envelope is invalid: {detail}")]
    InvalidEnvelope { detail: String },
    #[error("AMQP message format {message_format:#010x} is not supported")]
    UnsupportedMessageFormat { message_format: u32 },
    #[error("the Service Bus AMQP batch is invalid: {detail}")]
    InvalidBatch { detail: String },
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
