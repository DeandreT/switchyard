#![forbid(unsafe_code)]

use thiserror::Error;

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
