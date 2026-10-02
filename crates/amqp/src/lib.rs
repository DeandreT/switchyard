//! AMQP 1.0 wire types and connection drivers used by Switchyard.

#![forbid(unsafe_code)]

mod codec;
mod server;
mod types;
mod value_codec;

pub use crate::{
    codec::{
        AMQP_HEADER, AMQP_PROTOCOL_ID, Frame, MessageSizeError, ProtocolHeader, SASL_HEADER,
        SASL_PROTOCOL_ID, decode_message, decode_message_with_budget, encode_frame, encode_message,
        encode_message_with_max_size, read_frame, read_frame_with_max_size, read_protocol_header,
        write_frame, write_protocol_header,
    },
    server::{
        ConnectionOptions, Delivery, EngineError, IncomingAttach, IncomingSession, LinkEndpoint,
        MessageFormatDecoders, PendingSettlement, Receiver, RetainedDelivery, SaslAuthenticator,
        Sender, ServerConnection, ServerSession,
    },
    types::*,
    value_codec::MessageDecodeBudget,
};
pub use serde_amqp::{
    Value,
    described::Described,
    descriptor::Descriptor,
    primitives::{Array, Binary, OrderedMap, Symbol, Uuid},
};

#[cfg(feature = "test-client")]
pub use crate::server::{
    ClientConnection, ClientDelivery, ClientReceiver, ClientSender, ClientSession,
};
