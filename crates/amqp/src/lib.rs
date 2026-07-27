//! AMQP 1.0 wire types and connection drivers used by Switchyard.

#![forbid(unsafe_code)]

mod codec;
mod server;
mod types;

pub use crate::{
    codec::{
        AMQP_HEADER, AMQP_PROTOCOL_ID, Frame, ProtocolHeader, SASL_HEADER, SASL_PROTOCOL_ID,
        decode_message, encode_frame, encode_message, read_frame, read_protocol_header,
        write_frame, write_protocol_header,
    },
    server::{
        Delivery, EngineError, IncomingSession, LinkEndpoint, Receiver, SaslAuthenticator, Sender,
        ServerConnection, ServerSession,
    },
    types::*,
};
pub use serde_amqp::{
    Value,
    primitives::{Array, Binary, OrderedMap, Symbol, Uuid},
};

#[cfg(feature = "test-client")]
pub use crate::server::{
    ClientConnection, ClientDelivery, ClientReceiver, ClientSender, ClientSession,
};
