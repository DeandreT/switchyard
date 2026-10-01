use std::io;

#[cfg(test)]
use serde_amqp::primitives::Timestamp;
use serde_amqp::{
    Value,
    described::Described,
    descriptor::Descriptor,
    primitives::{Array, Binary, OrderedMap, Symbol},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::types::*;
use crate::value_codec::{MessageDecodeBudget, ValueDecoder, decode_value};

mod message_encoder;
mod transactions;
mod value_encoder;

use transactions::{target_terminus_from_value, target_terminus_to_value};

pub(crate) use message_encoder::prepare_message;
pub use message_encoder::{MessageSizeError, encode_message_with_max_size};

#[cfg(test)]
pub(crate) fn encoded_message_buffer_allocations() -> usize {
    value_encoder::buffer_allocations()
}

pub const AMQP_PROTOCOL_ID: u8 = 0;
pub const SASL_PROTOCOL_ID: u8 = 3;
pub const AMQP_HEADER: [u8; 8] = *b"AMQP\x00\x01\x00\x00";
pub const SASL_HEADER: [u8; 8] = *b"AMQP\x03\x01\x00\x00";

const AMQP_FRAME_TYPE: u8 = 0;
const SASL_FRAME_TYPE: u8 = 1;
const FRAME_HEADER_SIZE: usize = 8;
pub(crate) const MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
#[error("invalid AMQP frame size {size}; receive maximum is {maximum}")]
pub(crate) struct FrameSizeError {
    pub size: u32,
    pub maximum: u32,
}

const OPEN: u64 = 0x10;
const BEGIN: u64 = 0x11;
const ATTACH: u64 = 0x12;
const FLOW: u64 = 0x13;
const TRANSFER: u64 = 0x14;
const DISPOSITION: u64 = 0x15;
const DETACH: u64 = 0x16;
const END: u64 = 0x17;
const CLOSE: u64 = 0x18;
const ERROR: u64 = 0x1d;
const RECEIVED: u64 = 0x23;
const ACCEPTED: u64 = 0x24;
const REJECTED: u64 = 0x25;
const RELEASED: u64 = 0x26;
const MODIFIED: u64 = 0x27;
const SOURCE: u64 = 0x28;
const TARGET: u64 = 0x29;
const COORDINATOR: u64 = 0x30;
const DECLARE: u64 = 0x31;
const DISCHARGE: u64 = 0x32;
const DECLARED: u64 = 0x33;
const TRANSACTIONAL_STATE: u64 = 0x34;

const HEADER: u64 = 0x70;
const DELIVERY_ANNOTATIONS: u64 = 0x71;
const MESSAGE_ANNOTATIONS: u64 = 0x72;
const PROPERTIES: u64 = 0x73;
const APPLICATION_PROPERTIES: u64 = 0x74;
const DATA: u64 = 0x75;
const AMQP_SEQUENCE: u64 = 0x76;
const AMQP_VALUE: u64 = 0x77;
const FOOTER: u64 = 0x78;

const SASL_MECHANISMS: u64 = 0x40;
const SASL_INIT: u64 = 0x41;
const SASL_CHALLENGE: u64 = 0x42;
const SASL_RESPONSE: u64 = 0x43;
const SASL_OUTCOME: u64 = 0x44;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolHeader {
    pub protocol_id: u8,
    pub major: u8,
    pub minor: u8,
    pub revision: u8,
}

impl ProtocolHeader {
    pub const AMQP: Self = Self {
        protocol_id: AMQP_PROTOCOL_ID,
        major: 1,
        minor: 0,
        revision: 0,
    };
    pub const SASL: Self = Self {
        protocol_id: SASL_PROTOCOL_ID,
        major: 1,
        minor: 0,
        revision: 0,
    };

    fn bytes(self) -> [u8; 8] {
        [
            b'A',
            b'M',
            b'Q',
            b'P',
            self.protocol_id,
            self.major,
            self.minor,
            self.revision,
        ]
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Amqp {
        channel: u16,
        performative: Option<Performative>,
        payload: Vec<u8>,
    },
    Sasl(SaslPerformative),
}

pub async fn read_protocol_header<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> io::Result<ProtocolHeader> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes).await?;
    if &bytes[..4] != b"AMQP" {
        return Err(invalid_data("invalid AMQP protocol header"));
    }
    Ok(ProtocolHeader {
        protocol_id: bytes[4],
        major: bytes[5],
        minor: bytes[6],
        revision: bytes[7],
    })
}

pub async fn write_protocol_header<W: AsyncWrite + Unpin>(
    writer: &mut W,
    header: ProtocolHeader,
) -> io::Result<()> {
    writer.write_all(&header.bytes()).await
}

pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Frame> {
    read_frame_with_max_size(reader, MAX_FRAME_SIZE as u32).await
}

/// Enforces the local advertised receive limit before allocating a frame body.
/// The codec's own maximum remains a ceiling even for larger advertised limits.
pub async fn read_frame_with_max_size<R: AsyncRead + Unpin>(
    reader: &mut R,
    maximum_bytes: u32,
) -> io::Result<Frame> {
    let maximum = (maximum_bytes as usize).min(MAX_FRAME_SIZE);
    if maximum < FRAME_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AMQP frame receive limit is shorter than its header",
        ));
    }
    let mut size_bytes = [0_u8; 4];
    reader.read_exact(&mut size_bytes).await?;
    let size = u32::from_be_bytes(size_bytes) as usize;
    if !(FRAME_HEADER_SIZE..=maximum).contains(&size) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            FrameSizeError {
                size: size as u32,
                maximum: maximum as u32,
            },
        ));
    }

    let mut frame = vec![0_u8; size];
    frame[..4].copy_from_slice(&size_bytes);
    reader.read_exact(&mut frame[4..]).await?;
    decode_frame(&frame)
}

pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, frame: &Frame) -> io::Result<()> {
    writer.write_all(&encode_frame(frame)?).await
}

pub fn encode_frame(frame: &Frame) -> io::Result<Vec<u8>> {
    let (frame_type, channel, performative, payload) = match frame {
        Frame::Amqp {
            channel,
            performative,
            payload,
        } => (
            AMQP_FRAME_TYPE,
            *channel,
            performative
                .as_ref()
                .map(performative_to_value)
                .transpose()?,
            payload.as_slice(),
        ),
        Frame::Sasl(performative) => (
            SASL_FRAME_TYPE,
            0,
            Some(sasl_to_value(performative)?),
            &[][..],
        ),
    };

    let encoded = performative
        .as_ref()
        .map(encode_value)
        .transpose()?
        .unwrap_or_default();
    let size = FRAME_HEADER_SIZE
        .checked_add(encoded.len())
        .and_then(|size| size.checked_add(payload.len()))
        .ok_or_else(|| invalid_data("AMQP frame size overflow"))?;
    if size > MAX_FRAME_SIZE {
        return Err(invalid_data(format!("AMQP frame is too large: {size}")));
    }

    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(&(size as u32).to_be_bytes());
    bytes.extend_from_slice(&[2, frame_type]);
    bytes.extend_from_slice(&channel.to_be_bytes());
    bytes.extend_from_slice(&encoded);
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

fn decode_frame(frame: &[u8]) -> io::Result<Frame> {
    if frame.len() < FRAME_HEADER_SIZE {
        return Err(invalid_data("AMQP frame is shorter than its header"));
    }
    let size = u32::from_be_bytes(
        frame[..4]
            .try_into()
            .map_err(|_| invalid_data("invalid frame size"))?,
    ) as usize;
    if size != frame.len() {
        return Err(invalid_data("AMQP frame size does not match its bytes"));
    }
    let body_start = usize::from(frame[4])
        .checked_mul(4)
        .ok_or_else(|| invalid_data("invalid AMQP data offset"))?;
    if body_start < FRAME_HEADER_SIZE || body_start > frame.len() {
        return Err(invalid_data("invalid AMQP data offset"));
    }
    let channel = u16::from_be_bytes(
        frame[6..8]
            .try_into()
            .map_err(|_| invalid_data("invalid AMQP channel"))?,
    );
    let body = &frame[body_start..];

    match frame[5] {
        AMQP_FRAME_TYPE if body.is_empty() => Ok(Frame::Amqp {
            channel,
            performative: None,
            payload: Vec::new(),
        }),
        AMQP_FRAME_TYPE => {
            let (value, performative_len) = decode_value(body)?;
            Ok(Frame::Amqp {
                channel,
                performative: Some(performative_from_value(value)?),
                payload: body[performative_len..].to_vec(),
            })
        }
        SASL_FRAME_TYPE if channel == 0 && !body.is_empty() => {
            let (value, performative_len) = decode_value(body)?;
            if performative_len != body.len() {
                return Err(invalid_data("SASL frame carries trailing payload"));
            }
            Ok(Frame::Sasl(sasl_from_value(value)?))
        }
        SASL_FRAME_TYPE => Err(invalid_data("invalid SASL frame")),
        frame_type => Err(invalid_data(format!(
            "unsupported AMQP frame type {frame_type}"
        ))),
    }
}

pub fn encode_message(message: &Message) -> io::Result<Vec<u8>> {
    encode_message_with_max_size(message, usize::MAX)
}

#[cfg(test)]
fn encode_message_legacy(message: &Message) -> io::Result<Vec<u8>> {
    let mut encoded = Vec::new();
    if let Some(header) = &message.header {
        append_value(&mut encoded, header_to_value(header))?;
    }
    if let Some(annotations) = &message.delivery_annotations {
        append_value(
            &mut encoded,
            described(DELIVERY_ANNOTATIONS, annotations_to_value(annotations)),
        )?;
    }
    if let Some(annotations) = &message.message_annotations {
        append_value(
            &mut encoded,
            described(MESSAGE_ANNOTATIONS, annotations_to_value(annotations)),
        )?;
    }
    if let Some(properties) = &message.properties {
        append_value(&mut encoded, properties_to_value(properties))?;
    }
    if let Some(properties) = &message.application_properties {
        append_value(&mut encoded, application_properties_to_value(properties))?;
    }
    match &message.body {
        Body::Data(sections) => {
            for section in sections {
                append_value(
                    &mut encoded,
                    described(DATA, Value::Binary(section.clone())),
                )?;
            }
        }
        Body::Sequence(sections) => {
            for section in sections {
                append_value(
                    &mut encoded,
                    described(AMQP_SEQUENCE, Value::List(section.clone())),
                )?;
            }
        }
        Body::Value(value) => {
            append_value(&mut encoded, described(AMQP_VALUE, value.clone()))?;
        }
        Body::Empty => {}
    }
    if let Some(footer) = &message.footer {
        append_value(
            &mut encoded,
            described(FOOTER, annotations_to_value(footer)),
        )?;
    }
    Ok(encoded)
}

pub fn decode_message(encoded: &[u8]) -> io::Result<Message> {
    decode_message_with_budget(encoded, &mut MessageDecodeBudget::default())
}

/// Decodes a message without resetting the caller's cumulative allocation budget.
/// Failed decoding does not refund charges already made.
pub fn decode_message_with_budget(
    encoded: &[u8],
    budget: &mut MessageDecodeBudget,
) -> io::Result<Message> {
    let mut message = Message::default();
    let mut decoder = ValueDecoder::new(encoded, budget);
    let mut offset = 0;
    let mut previous_section = None;
    while offset < encoded.len() {
        let (value, len) = decoder.next_value()?;
        offset += len;

        let (descriptor, value) = take_described(value)?;
        let section = match descriptor {
            HEADER => 0,
            DELIVERY_ANNOTATIONS => 1,
            MESSAGE_ANNOTATIONS => 2,
            PROPERTIES => 3,
            APPLICATION_PROPERTIES => 4,
            DATA | AMQP_SEQUENCE | AMQP_VALUE => 5,
            FOOTER => 6,
            _ => return Err(invalid_data("unknown message section")),
        };
        if previous_section
            .is_some_and(|previous| section < previous || (section == previous && section != 5))
        {
            return Err(invalid_data("message sections repeat or are out of order"));
        }
        previous_section = Some(section);
        match descriptor {
            HEADER => message.header = Some(header_from_value(value)?),
            DELIVERY_ANNOTATIONS => {
                message.delivery_annotations = Some(annotations_from_value(value)?);
            }
            MESSAGE_ANNOTATIONS => {
                message.message_annotations = Some(annotations_from_value(value)?);
            }
            PROPERTIES => message.properties = Some(properties_from_value(value)?),
            APPLICATION_PROPERTIES => {
                message.application_properties = Some(application_properties_from_value(value)?);
            }
            DATA => {
                let Value::Binary(section) = value else {
                    return Err(invalid_data("data section is not binary"));
                };
                match &mut message.body {
                    Body::Empty => message.body = Body::Data(vec![section]),
                    Body::Data(sections) => sections.push(section),
                    _ => return Err(invalid_data("message mixes body section types")),
                }
            }
            AMQP_SEQUENCE => {
                let Value::List(sequence) = value else {
                    return Err(invalid_data("AMQP sequence body is not a list"));
                };
                match &mut message.body {
                    Body::Empty => message.body = Body::Sequence(vec![sequence]),
                    Body::Sequence(sections) => sections.push(sequence),
                    _ => return Err(invalid_data("message mixes body section types")),
                }
            }
            AMQP_VALUE => {
                if !matches!(message.body, Body::Empty) {
                    return Err(invalid_data(
                        "message has multiple or mixed value body sections",
                    ));
                }
                message.body = Body::Value(value);
            }
            FOOTER => message.footer = Some(annotations_from_value(value)?),
            _ => unreachable!("message sections validated above"),
        }
    }
    Ok(message)
}

fn performative_to_value(performative: &Performative) -> io::Result<Value> {
    Ok(match performative {
        Performative::Open(open) => described(
            OPEN,
            list(vec![
                Value::String(open.container_id.clone()),
                optional_string(&open.hostname),
                Value::Uint(open.max_frame_size),
                Value::Ushort(open.channel_max),
                optional_u32(open.idle_time_out),
                symbol_array(&open.outgoing_locales),
                symbol_array(&open.incoming_locales),
                symbol_array(&open.offered_capabilities),
                symbol_array(&open.desired_capabilities),
                fields_to_value(&open.properties),
            ]),
        ),
        Performative::Begin(begin) => described(
            BEGIN,
            list(vec![
                begin
                    .remote_channel
                    .map(Value::Ushort)
                    .unwrap_or(Value::Null),
                Value::Uint(begin.next_outgoing_id),
                Value::Uint(begin.incoming_window),
                Value::Uint(begin.outgoing_window),
                Value::Uint(begin.handle_max),
                symbol_array(&begin.offered_capabilities),
                symbol_array(&begin.desired_capabilities),
                fields_to_value(&begin.properties),
            ]),
        ),
        Performative::Attach(attach) => described(
            ATTACH,
            list(vec![
                Value::String(attach.name.clone()),
                Value::Uint(attach.handle),
                attach.role.to_value(),
                attach.snd_settle_mode.to_value(),
                attach.rcv_settle_mode.to_value(),
                attach
                    .source
                    .as_ref()
                    .map(source_to_value)
                    .transpose()?
                    .unwrap_or(Value::Null),
                attach
                    .target
                    .as_ref()
                    .map(target_terminus_to_value)
                    .transpose()?
                    .unwrap_or(Value::Null),
                unsettled_to_value(&attach.unsettled)?,
                Value::Bool(attach.incomplete_unsettled),
                optional_u32(attach.initial_delivery_count),
                attach
                    .max_message_size
                    .map(Value::Ulong)
                    .unwrap_or(Value::Null),
                symbol_array(&attach.offered_capabilities),
                symbol_array(&attach.desired_capabilities),
                fields_to_value(&attach.properties),
            ]),
        ),
        Performative::Flow(flow) => described(
            FLOW,
            list(vec![
                optional_u32(flow.next_incoming_id),
                Value::Uint(flow.incoming_window),
                Value::Uint(flow.next_outgoing_id),
                Value::Uint(flow.outgoing_window),
                optional_u32(flow.handle),
                optional_u32(flow.delivery_count),
                optional_u32(flow.link_credit),
                optional_u32(flow.available),
                Value::Bool(flow.drain),
                Value::Bool(flow.echo),
                fields_to_value(&flow.properties),
            ]),
        ),
        Performative::Transfer(transfer) => described(
            TRANSFER,
            list(vec![
                Value::Uint(transfer.handle),
                optional_u32(transfer.delivery_id),
                transfer
                    .delivery_tag
                    .as_ref()
                    .map(|tag| Value::Binary(tag.clone()))
                    .unwrap_or(Value::Null),
                optional_u32(transfer.message_format),
                transfer.settled.map(Value::Bool).unwrap_or(Value::Null),
                Value::Bool(transfer.more),
                transfer
                    .rcv_settle_mode
                    .as_ref()
                    .map(ReceiverSettleMode::to_value)
                    .unwrap_or(Value::Null),
                transfer
                    .state
                    .as_ref()
                    .map(delivery_state_to_value)
                    .transpose()?
                    .unwrap_or(Value::Null),
                Value::Bool(transfer.resume),
                Value::Bool(transfer.aborted),
                Value::Bool(transfer.batchable),
            ]),
        ),
        Performative::Disposition(disposition) => described(
            DISPOSITION,
            list(vec![
                disposition.role.to_value(),
                Value::Uint(disposition.first),
                optional_u32(disposition.last),
                Value::Bool(disposition.settled),
                disposition
                    .state
                    .as_ref()
                    .map(delivery_state_to_value)
                    .transpose()?
                    .unwrap_or(Value::Null),
                Value::Bool(disposition.batchable),
            ]),
        ),
        Performative::Detach(detach) => described(
            DETACH,
            list(vec![
                Value::Uint(detach.handle),
                Value::Bool(detach.closed),
                detach
                    .error
                    .as_ref()
                    .map(error_to_value)
                    .unwrap_or(Value::Null),
            ]),
        ),
        Performative::End(end) => described(
            END,
            list(vec![
                end.error
                    .as_ref()
                    .map(error_to_value)
                    .unwrap_or(Value::Null),
            ]),
        ),
        Performative::Close(close) => described(
            CLOSE,
            list(vec![
                close
                    .error
                    .as_ref()
                    .map(error_to_value)
                    .unwrap_or(Value::Null),
            ]),
        ),
    })
}

fn performative_from_value(value: Value) -> io::Result<Performative> {
    let (descriptor, value) = take_described(value)?;
    let fields = take_list(value)?;
    Ok(match descriptor {
        OPEN => Performative::Open(Open {
            container_id: required_string(&fields, 0, "open.container-id")?,
            hostname: string_field(&fields, 1)?,
            max_frame_size: u32_field(&fields, 2)?.unwrap_or(262_144),
            channel_max: u16_field(&fields, 3)?.unwrap_or(u16::MAX),
            idle_time_out: u32_field(&fields, 4)?,
            outgoing_locales: symbol_array_field(&fields, 5)?,
            incoming_locales: symbol_array_field(&fields, 6)?,
            offered_capabilities: symbol_array_field(&fields, 7)?,
            desired_capabilities: symbol_array_field(&fields, 8)?,
            properties: fields_field(&fields, 9)?,
        }),
        BEGIN => Performative::Begin(Begin {
            remote_channel: u16_field(&fields, 0)?,
            next_outgoing_id: required_u32(&fields, 1, "begin.next-outgoing-id")?,
            incoming_window: required_u32(&fields, 2, "begin.incoming-window")?,
            outgoing_window: required_u32(&fields, 3, "begin.outgoing-window")?,
            handle_max: u32_field(&fields, 4)?.unwrap_or(u32::MAX),
            offered_capabilities: symbol_array_field(&fields, 5)?,
            desired_capabilities: symbol_array_field(&fields, 6)?,
            properties: fields_field(&fields, 7)?,
        }),
        ATTACH => Performative::Attach(Box::new(Attach {
            name: required_string(&fields, 0, "attach.name")?,
            handle: required_u32(&fields, 1, "attach.handle")?,
            role: Role::from_value(field(&fields, 2))
                .ok_or_else(|| invalid_data("invalid attach role"))?,
            snd_settle_mode: match field(&fields, 3) {
                Value::Null => SenderSettleMode::Mixed,
                value => SenderSettleMode::from_value(value)
                    .ok_or_else(|| invalid_data("invalid sender settle mode"))?,
            },
            rcv_settle_mode: match field(&fields, 4) {
                Value::Null => ReceiverSettleMode::First,
                value => ReceiverSettleMode::from_value(value)
                    .ok_or_else(|| invalid_data("invalid receiver settle mode"))?,
            },
            source: match field(&fields, 5) {
                Value::Null => None,
                value => Some(source_from_value(value)?),
            },
            target: match field(&fields, 6) {
                Value::Null => None,
                value => Some(target_terminus_from_value(value)?),
            },
            unsettled: unsettled_from_value(field(&fields, 7))?,
            incomplete_unsettled: bool_field(&fields, 8)?.unwrap_or(false),
            initial_delivery_count: u32_field(&fields, 9)?,
            max_message_size: u64_field(&fields, 10)?,
            offered_capabilities: symbol_array_field(&fields, 11)?,
            desired_capabilities: symbol_array_field(&fields, 12)?,
            properties: fields_field(&fields, 13)?,
        })),
        FLOW => Performative::Flow(Flow {
            next_incoming_id: u32_field(&fields, 0)?,
            incoming_window: required_u32(&fields, 1, "flow.incoming-window")?,
            next_outgoing_id: required_u32(&fields, 2, "flow.next-outgoing-id")?,
            outgoing_window: required_u32(&fields, 3, "flow.outgoing-window")?,
            handle: u32_field(&fields, 4)?,
            delivery_count: u32_field(&fields, 5)?,
            link_credit: u32_field(&fields, 6)?,
            available: u32_field(&fields, 7)?,
            drain: bool_field(&fields, 8)?.unwrap_or(false),
            echo: bool_field(&fields, 9)?.unwrap_or(false),
            properties: fields_field(&fields, 10)?,
        }),
        TRANSFER => Performative::Transfer(Transfer {
            handle: required_u32(&fields, 0, "transfer.handle")?,
            delivery_id: u32_field(&fields, 1)?,
            delivery_tag: binary_field(&fields, 2)?,
            message_format: u32_field(&fields, 3)?,
            settled: bool_field(&fields, 4)?,
            more: bool_field(&fields, 5)?.unwrap_or(false),
            rcv_settle_mode: match field(&fields, 6) {
                Value::Null => None,
                value => Some(
                    ReceiverSettleMode::from_value(value)
                        .ok_or_else(|| invalid_data("invalid transfer settle mode"))?,
                ),
            },
            state: match field(&fields, 7) {
                Value::Null => None,
                value => Some(delivery_state_from_value(value)?),
            },
            resume: bool_field(&fields, 8)?.unwrap_or(false),
            aborted: bool_field(&fields, 9)?.unwrap_or(false),
            batchable: bool_field(&fields, 10)?.unwrap_or(false),
        }),
        DISPOSITION => Performative::Disposition(Disposition {
            role: Role::from_value(field(&fields, 0))
                .ok_or_else(|| invalid_data("invalid disposition role"))?,
            first: required_u32(&fields, 1, "disposition.first")?,
            last: u32_field(&fields, 2)?,
            settled: bool_field(&fields, 3)?.unwrap_or(false),
            state: match field(&fields, 4) {
                Value::Null => None,
                value => Some(delivery_state_from_value(value)?),
            },
            batchable: bool_field(&fields, 5)?.unwrap_or(false),
        }),
        DETACH => Performative::Detach(Detach {
            handle: required_u32(&fields, 0, "detach.handle")?,
            closed: bool_field(&fields, 1)?.unwrap_or(false),
            error: error_field(&fields, 2)?,
        }),
        END => Performative::End(End {
            error: error_field(&fields, 0)?,
        }),
        CLOSE => Performative::Close(Close {
            error: error_field(&fields, 0)?,
        }),
        other => {
            return Err(invalid_data(format!(
                "unknown AMQP performative {other:#x}"
            )));
        }
    })
}

fn sasl_to_value(performative: &SaslPerformative) -> io::Result<Value> {
    Ok(match performative {
        SaslPerformative::Mechanisms(mechanisms) => described(
            SASL_MECHANISMS,
            list(vec![Value::Array(Array::from(
                mechanisms
                    .mechanisms
                    .iter()
                    .cloned()
                    .map(Value::Symbol)
                    .collect::<Vec<_>>(),
            ))]),
        ),
        SaslPerformative::Init(init) => described(
            SASL_INIT,
            list(vec![
                Value::Symbol(init.mechanism.clone()),
                init.initial_response
                    .as_ref()
                    .map(|response| Value::Binary(response.clone()))
                    .unwrap_or(Value::Null),
                optional_string(&init.hostname),
            ]),
        ),
        SaslPerformative::Challenge(challenge) => described(
            SASL_CHALLENGE,
            list(vec![Value::Binary(challenge.challenge.clone())]),
        ),
        SaslPerformative::Response(response) => described(
            SASL_RESPONSE,
            list(vec![Value::Binary(response.response.clone())]),
        ),
        SaslPerformative::Outcome(outcome) => described(
            SASL_OUTCOME,
            list(vec![
                Value::Ubyte(match outcome.code {
                    SaslCode::Ok => 0,
                    SaslCode::Auth => 1,
                    SaslCode::Sys => 2,
                    SaslCode::SysPerm => 3,
                    SaslCode::SysTemp => 4,
                }),
                outcome
                    .additional_data
                    .as_ref()
                    .map(|data| Value::Binary(data.clone()))
                    .unwrap_or(Value::Null),
            ]),
        ),
    })
}

fn sasl_from_value(value: Value) -> io::Result<SaslPerformative> {
    let (descriptor, value) = take_described(value)?;
    let fields = take_list(value)?;
    Ok(match descriptor {
        SASL_MECHANISMS => {
            let Some(mechanisms) = symbol_array_field(&fields, 0)? else {
                return Err(invalid_data("SASL mechanisms are required"));
            };
            SaslPerformative::Mechanisms(SaslMechanisms {
                mechanisms: mechanisms.into_inner(),
            })
        }
        SASL_INIT => SaslPerformative::Init(SaslInit {
            mechanism: match field(&fields, 0) {
                Value::Symbol(value) => value,
                _ => return Err(invalid_data("SASL mechanism is required")),
            },
            initial_response: binary_field(&fields, 1)?,
            hostname: string_field(&fields, 2)?,
        }),
        SASL_CHALLENGE => SaslPerformative::Challenge(SaslChallenge {
            challenge: binary_field(&fields, 0)?
                .ok_or_else(|| invalid_data("SASL challenge is required"))?,
        }),
        SASL_RESPONSE => SaslPerformative::Response(SaslResponse {
            response: binary_field(&fields, 0)?
                .ok_or_else(|| invalid_data("SASL response is required"))?,
        }),
        SASL_OUTCOME => SaslPerformative::Outcome(SaslOutcome {
            code: match field(&fields, 0) {
                Value::Ubyte(0) => SaslCode::Ok,
                Value::Ubyte(1) => SaslCode::Auth,
                Value::Ubyte(2) => SaslCode::Sys,
                Value::Ubyte(3) => SaslCode::SysPerm,
                Value::Ubyte(4) => SaslCode::SysTemp,
                _ => return Err(invalid_data("invalid SASL outcome code")),
            },
            additional_data: binary_field(&fields, 1)?,
        }),
        other => {
            return Err(invalid_data(format!(
                "unknown SASL performative {other:#x}"
            )));
        }
    })
}

fn source_to_value(source: &Source) -> io::Result<Value> {
    let default_outcome = match source.default_outcome.as_ref() {
        Some(state) => {
            validate_default_outcome(state)?;
            delivery_state_to_value(state)?
        }
        None => Value::Null,
    };
    Ok(described(
        SOURCE,
        list(vec![
            optional_string(&source.address),
            Value::Uint(source.durable),
            source
                .expiry_policy
                .as_ref()
                .map(|value| Value::Symbol(value.clone()))
                .unwrap_or(Value::Null),
            Value::Uint(source.timeout),
            Value::Bool(source.dynamic),
            fields_to_value(&source.dynamic_node_properties),
            source
                .distribution_mode
                .as_ref()
                .map(|value| Value::Symbol(value.clone()))
                .unwrap_or(Value::Null),
            fields_to_value(&source.filter),
            default_outcome,
            symbol_array(&source.outcomes),
            symbol_array(&source.capabilities),
        ]),
    ))
}

fn validate_default_outcome(state: &DeliveryState) -> io::Result<()> {
    if !state.is_terminal() {
        return Err(invalid_data("source default outcome must be terminal"));
    }
    Ok(())
}

fn source_from_value(value: Value) -> io::Result<Source> {
    let (descriptor, value) = take_described(value)?;
    if descriptor != SOURCE {
        return Err(invalid_data("terminus is not an AMQP source"));
    }
    let fields = take_list(value)?;
    Ok(Source {
        address: string_field(&fields, 0)?,
        durable: u32_field(&fields, 1)?.unwrap_or(0),
        expiry_policy: symbol_field(&fields, 2)?,
        timeout: u32_field(&fields, 3)?.unwrap_or(0),
        dynamic: bool_field(&fields, 4)?.unwrap_or(false),
        dynamic_node_properties: fields_field(&fields, 5)?,
        distribution_mode: symbol_field(&fields, 6)?,
        filter: fields_field(&fields, 7)?,
        default_outcome: match field(&fields, 8) {
            Value::Null => None,
            value => {
                let state = delivery_state_from_value(value)?;
                validate_default_outcome(&state)?;
                Some(state)
            }
        },
        outcomes: symbol_array_field(&fields, 9)?,
        capabilities: symbol_array_field(&fields, 10)?,
    })
}

fn target_to_value(target: &Target) -> Value {
    described(
        TARGET,
        list(vec![
            optional_string(&target.address),
            Value::Uint(target.durable),
            target
                .expiry_policy
                .as_ref()
                .map(|value| Value::Symbol(value.clone()))
                .unwrap_or(Value::Null),
            Value::Uint(target.timeout),
            Value::Bool(target.dynamic),
            fields_to_value(&target.dynamic_node_properties),
            symbol_array(&target.capabilities),
        ]),
    )
}

fn target_from_fields(fields: Vec<Value>) -> io::Result<Target> {
    Ok(Target {
        address: string_field(&fields, 0)?,
        durable: u32_field(&fields, 1)?.unwrap_or(0),
        expiry_policy: symbol_field(&fields, 2)?,
        timeout: u32_field(&fields, 3)?.unwrap_or(0),
        dynamic: bool_field(&fields, 4)?.unwrap_or(false),
        dynamic_node_properties: fields_field(&fields, 5)?,
        capabilities: symbol_array_field(&fields, 6)?,
    })
}

fn delivery_state_to_value(state: &DeliveryState) -> io::Result<Value> {
    Ok(match state {
        DeliveryState::Received {
            section_number,
            section_offset,
        } => described(
            RECEIVED,
            list(vec![
                Value::Uint(*section_number),
                Value::Ulong(*section_offset),
            ]),
        ),
        DeliveryState::Accepted(_) => described(ACCEPTED, Value::List(Vec::new())),
        DeliveryState::Rejected(rejected) => described(
            REJECTED,
            list(vec![
                rejected
                    .error
                    .as_ref()
                    .map(error_to_value)
                    .unwrap_or(Value::Null),
            ]),
        ),
        DeliveryState::Released(_) => described(RELEASED, Value::List(Vec::new())),
        DeliveryState::Modified(modified) => described(
            MODIFIED,
            list(vec![
                modified
                    .delivery_failed
                    .map(Value::Bool)
                    .unwrap_or(Value::Null),
                modified
                    .undeliverable_here
                    .map(Value::Bool)
                    .unwrap_or(Value::Null),
                fields_to_value(&modified.message_annotations),
            ]),
        ),
        DeliveryState::Declared(declared) => transactions::declared_to_value(declared),
        DeliveryState::Transactional(state) => transactions::transactional_state_to_value(state),
    })
}

fn delivery_state_from_value(value: Value) -> io::Result<DeliveryState> {
    let (descriptor, value) = take_described(value)?;
    let fields = take_list(value)?;
    delivery_state_from_fields(descriptor, fields)
}

fn delivery_state_from_fields(descriptor: u64, fields: Vec<Value>) -> io::Result<DeliveryState> {
    Ok(match descriptor {
        RECEIVED => DeliveryState::Received {
            section_number: required_u32(&fields, 0, "received.section-number")?,
            section_offset: required_u64(&fields, 1, "received.section-offset")?,
        },
        ACCEPTED => DeliveryState::Accepted(Accepted),
        REJECTED => DeliveryState::Rejected(Rejected {
            error: error_field(&fields, 0)?,
        }),
        RELEASED => DeliveryState::Released(Released),
        MODIFIED => DeliveryState::Modified(Modified {
            delivery_failed: bool_field(&fields, 0)?,
            undeliverable_here: bool_field(&fields, 1)?,
            message_annotations: fields_field(&fields, 2)?,
        }),
        DECLARED => DeliveryState::Declared(transactions::declared_from_fields(&fields)?),
        TRANSACTIONAL_STATE => {
            DeliveryState::Transactional(transactions::transactional_state_from_fields(fields)?)
        }
        other => return Err(invalid_data(format!("unknown delivery state {other:#x}"))),
    })
}

fn error_to_value(error: &Error) -> Value {
    described(
        ERROR,
        list(vec![
            Value::Symbol(error.condition.as_symbol()),
            error
                .description
                .as_ref()
                .map(|description| Value::String(description.clone()))
                .unwrap_or(Value::Null),
            fields_to_value(&error.info),
        ]),
    )
}

fn error_from_value(value: Value) -> io::Result<Error> {
    let (descriptor, value) = take_described(value)?;
    if descriptor != ERROR {
        return Err(invalid_data("value is not an AMQP error"));
    }
    let fields = take_list(value)?;
    let condition = match field(&fields, 0) {
        Value::Symbol(symbol) => AmqpError::from_symbol(symbol.as_str())
            .map(ErrorCondition::Amqp)
            .unwrap_or(ErrorCondition::Custom(symbol)),
        _ => return Err(invalid_data("AMQP error condition is required")),
    };
    Ok(Error {
        condition,
        description: string_field(&fields, 1)?,
        info: fields_field(&fields, 2)?,
    })
}

#[cfg(test)]
fn header_to_value(header: &Header) -> Value {
    described(
        HEADER,
        list(vec![
            Value::Bool(header.durable),
            Value::Ubyte(header.priority),
            optional_u32(header.ttl),
            Value::Bool(header.first_acquirer),
            Value::Uint(header.delivery_count),
        ]),
    )
}

fn header_from_value(value: Value) -> io::Result<Header> {
    let fields = take_list(value)?;
    Ok(Header {
        durable: bool_field(&fields, 0)?.unwrap_or(false),
        priority: u8_field(&fields, 1)?.unwrap_or(4),
        ttl: u32_field(&fields, 2)?,
        first_acquirer: bool_field(&fields, 3)?.unwrap_or(false),
        delivery_count: u32_field(&fields, 4)?.unwrap_or(0),
    })
}

#[cfg(test)]
fn properties_to_value(properties: &Properties) -> Value {
    described(
        PROPERTIES,
        list(vec![
            properties
                .message_id
                .as_ref()
                .map(message_id_to_value)
                .unwrap_or(Value::Null),
            properties
                .user_id
                .as_ref()
                .map(|value| Value::Binary(value.clone()))
                .unwrap_or(Value::Null),
            optional_string(&properties.to),
            optional_string(&properties.subject),
            optional_string(&properties.reply_to),
            properties
                .correlation_id
                .as_ref()
                .map(message_id_to_value)
                .unwrap_or(Value::Null),
            properties
                .content_type
                .as_ref()
                .map(|value| Value::Symbol(value.clone()))
                .unwrap_or(Value::Null),
            properties
                .content_encoding
                .as_ref()
                .map(|value| Value::Symbol(value.clone()))
                .unwrap_or(Value::Null),
            properties
                .absolute_expiry_time
                .map(|value| Value::Timestamp(Timestamp::from_milliseconds(value)))
                .unwrap_or(Value::Null),
            properties
                .creation_time
                .map(|value| Value::Timestamp(Timestamp::from_milliseconds(value)))
                .unwrap_or(Value::Null),
            optional_string(&properties.group_id),
            optional_u32(properties.group_sequence),
            optional_string(&properties.reply_to_group_id),
        ]),
    )
}

fn properties_from_value(value: Value) -> io::Result<Properties> {
    let fields = take_list(value)?;
    Ok(Properties {
        message_id: message_id_field(&fields, 0)?,
        user_id: binary_field(&fields, 1)?,
        to: string_field(&fields, 2)?,
        subject: string_field(&fields, 3)?,
        reply_to: string_field(&fields, 4)?,
        correlation_id: message_id_field(&fields, 5)?,
        content_type: symbol_field(&fields, 6)?,
        content_encoding: symbol_field(&fields, 7)?,
        absolute_expiry_time: i64_field(&fields, 8)?,
        creation_time: i64_field(&fields, 9)?,
        group_id: string_field(&fields, 10)?,
        group_sequence: u32_field(&fields, 11)?,
        reply_to_group_id: string_field(&fields, 12)?,
    })
}

#[cfg(test)]
fn application_properties_to_value(properties: &ApplicationProperties) -> Value {
    let mut map = OrderedMap::new();
    for (key, value) in properties.0.iter() {
        map.insert(Value::String(key.clone()), value.clone());
    }
    described(APPLICATION_PROPERTIES, Value::Map(map))
}

fn application_properties_from_value(value: Value) -> io::Result<ApplicationProperties> {
    let Value::Map(map) = value else {
        return Err(invalid_data("application properties are not a map"));
    };
    let mut properties = OrderedMap::new();
    for (key, value) in map {
        let Value::String(key) = key else {
            return Err(invalid_data("application property key is not a string"));
        };
        properties.insert(key, value);
    }
    Ok(ApplicationProperties(properties))
}

#[cfg(test)]
fn message_id_to_value(message_id: &MessageId) -> Value {
    match message_id {
        MessageId::Ulong(value) => Value::Ulong(*value),
        MessageId::Uuid(value) => Value::Uuid(value.clone()),
        MessageId::Binary(value) => Value::Binary(value.clone()),
        MessageId::String(value) => Value::String(value.clone()),
    }
}

fn message_id_field(fields: &[Value], index: usize) -> io::Result<Option<MessageId>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Ulong(value) => Some(MessageId::Ulong(value)),
        Value::Uuid(value) => Some(MessageId::Uuid(value)),
        Value::Binary(value) => Some(MessageId::Binary(value)),
        Value::String(value) => Some(MessageId::String(value)),
        _ => return Err(invalid_data("invalid AMQP message id")),
    })
}

fn unsettled_to_value(
    unsettled: &Option<OrderedMap<DeliveryTag, Option<DeliveryState>>>,
) -> io::Result<Value> {
    let Some(unsettled) = unsettled else {
        return Ok(Value::Null);
    };
    let mut map = OrderedMap::new();
    for (tag, state) in unsettled.iter() {
        map.insert(
            Value::Binary(tag.clone()),
            state
                .as_ref()
                .map(delivery_state_to_value)
                .transpose()?
                .unwrap_or(Value::Null),
        );
    }
    Ok(Value::Map(map))
}

fn unsettled_from_value(
    value: Value,
) -> io::Result<Option<OrderedMap<DeliveryTag, Option<DeliveryState>>>> {
    let Value::Map(map) = value else {
        return if value == Value::Null {
            Ok(None)
        } else {
            Err(invalid_data("attach unsettled field is not a map"))
        };
    };
    let mut unsettled = OrderedMap::new();
    for (tag, state) in map {
        let Value::Binary(tag) = tag else {
            return Err(invalid_data("unsettled delivery tag is not binary"));
        };
        let state = if state == Value::Null {
            None
        } else {
            Some(delivery_state_from_value(state)?)
        };
        unsettled.insert(tag, state);
    }
    Ok(Some(unsettled))
}

fn fields_to_value(fields: &Option<impl FieldKey>) -> Value {
    let Some(fields) = fields else {
        return Value::Null;
    };
    let mut map = OrderedMap::new();
    for (key, value) in fields.entries() {
        map.insert(Value::Symbol(key.clone()), value.clone());
    }
    Value::Map(map)
}

#[cfg(test)]
fn annotations_to_value(annotations: &Annotations) -> Value {
    Value::Map(
        annotations
            .iter()
            .map(|(key, value)| {
                let key = match key {
                    AnnotationKey::Symbol(key) => Value::Symbol(key.clone()),
                    AnnotationKey::Ulong(key) => Value::Ulong(*key),
                };
                (key, value.clone())
            })
            .collect(),
    )
}

fn annotations_from_value(value: Value) -> io::Result<Annotations> {
    let Value::Map(map) = value else {
        return Err(invalid_data("annotations section is not a map"));
    };
    let mut annotations = Annotations::new();
    for (key, value) in map {
        let key = match key {
            Value::Symbol(key) => AnnotationKey::Symbol(key),
            Value::Ulong(key) => AnnotationKey::Ulong(key),
            _ => return Err(invalid_data("annotation key is not a symbol or ulong")),
        };
        annotations.insert(key, value);
    }
    Ok(annotations)
}

trait FieldKey {
    fn entries(&self) -> impl Iterator<Item = (&Symbol, &Value)>;
}

impl FieldKey for Fields {
    fn entries(&self) -> impl Iterator<Item = (&Symbol, &Value)> {
        self.iter()
    }
}

fn fields_field(fields: &[Value], index: usize) -> io::Result<Option<Fields>> {
    let value = field(fields, index);
    fields_from_value(value)
}

fn fields_from_value(value: Value) -> io::Result<Option<Fields>> {
    if value == Value::Null {
        return Ok(None);
    }
    let Value::Map(map) = value else {
        return Err(invalid_data("AMQP fields value is not a map"));
    };
    let mut fields = Fields::new();
    for (key, value) in map {
        let Value::Symbol(key) = key else {
            return Err(invalid_data("AMQP fields key is not a symbol"));
        };
        fields.insert(key, value);
    }
    Ok(Some(fields))
}

fn symbol_array(array: &Option<Array<Symbol>>) -> Value {
    array
        .as_ref()
        .filter(|array| !array.is_empty())
        .map_or(Value::Null, |array| {
            Value::Array(Array::from(
                array.iter().cloned().map(Value::Symbol).collect::<Vec<_>>(),
            ))
        })
}

fn symbol_array_field(fields: &[Value], index: usize) -> io::Result<Option<Array<Symbol>>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Array(values) => Some(Array::from(
            values
                .into_iter()
                .map(|value| match value {
                    Value::Symbol(value) => Ok(value),
                    _ => Err(invalid_data("array element is not a symbol")),
                })
                .collect::<io::Result<Vec<_>>>()?,
        )),
        _ => return Err(invalid_data("value is not a symbol array")),
    })
}

fn described(code: u64, value: Value) -> Value {
    Value::Described(Box::new(Described {
        descriptor: Descriptor::Code(code),
        value,
    }))
}

fn take_described(value: Value) -> io::Result<(u64, Value)> {
    let Value::Described(value) = value else {
        return Err(invalid_data("AMQP value is not described"));
    };
    let code = match value.descriptor {
        Descriptor::Code(code) => code,
        Descriptor::Name(name) => match name.as_str() {
            "amqp:open:list" => OPEN,
            "amqp:begin:list" => BEGIN,
            "amqp:attach:list" => ATTACH,
            "amqp:flow:list" => FLOW,
            "amqp:transfer:list" => TRANSFER,
            "amqp:disposition:list" => DISPOSITION,
            "amqp:detach:list" => DETACH,
            "amqp:end:list" => END,
            "amqp:close:list" => CLOSE,
            "amqp:error:list" => ERROR,
            "amqp:received:list" => RECEIVED,
            "amqp:accepted:list" => ACCEPTED,
            "amqp:rejected:list" => REJECTED,
            "amqp:released:list" => RELEASED,
            "amqp:modified:list" => MODIFIED,
            "amqp:source:list" => SOURCE,
            "amqp:target:list" => TARGET,
            "amqp:coordinator:list" => COORDINATOR,
            "amqp:declare:list" => DECLARE,
            "amqp:discharge:list" => DISCHARGE,
            "amqp:declared:list" => DECLARED,
            "amqp:transactional-state:list" => TRANSACTIONAL_STATE,
            "amqp:header:list" => HEADER,
            "amqp:delivery-annotations:map" => DELIVERY_ANNOTATIONS,
            "amqp:message-annotations:map" => MESSAGE_ANNOTATIONS,
            "amqp:properties:list" => PROPERTIES,
            "amqp:application-properties:map" => APPLICATION_PROPERTIES,
            "amqp:data:binary" => DATA,
            "amqp:amqp-sequence:list" => AMQP_SEQUENCE,
            "amqp:amqp-value:*" => AMQP_VALUE,
            "amqp:footer:map" => FOOTER,
            "amqp:sasl-mechanisms:list" => SASL_MECHANISMS,
            "amqp:sasl-init:list" => SASL_INIT,
            "amqp:sasl-challenge:list" => SASL_CHALLENGE,
            "amqp:sasl-response:list" => SASL_RESPONSE,
            "amqp:sasl-outcome:list" => SASL_OUTCOME,
            _ => return Err(invalid_data("unknown symbolic descriptor")),
        },
    };
    Ok((code, value.value))
}

fn list(mut fields: Vec<Value>) -> Value {
    while fields.last() == Some(&Value::Null) {
        fields.pop();
    }
    Value::List(fields)
}

fn take_list(value: Value) -> io::Result<Vec<Value>> {
    match value {
        Value::List(fields) => Ok(fields),
        _ => Err(invalid_data("described AMQP value is not list encoded")),
    }
}

fn field(fields: &[Value], index: usize) -> Value {
    fields.get(index).cloned().unwrap_or(Value::Null)
}

fn required_string(fields: &[Value], index: usize, name: &str) -> io::Result<String> {
    string_field(fields, index)?.ok_or_else(|| invalid_data(format!("{name} is required")))
}

fn string_field(fields: &[Value], index: usize) -> io::Result<Option<String>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::String(value) => Some(value),
        _ => return Err(invalid_data("value is not a string")),
    })
}

fn symbol_field(fields: &[Value], index: usize) -> io::Result<Option<Symbol>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Symbol(value) => Some(value),
        _ => return Err(invalid_data("value is not a symbol")),
    })
}

fn binary_field(fields: &[Value], index: usize) -> io::Result<Option<Binary>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Binary(value) => Some(value),
        _ => return Err(invalid_data("value is not binary")),
    })
}

fn bool_field(fields: &[Value], index: usize) -> io::Result<Option<bool>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Bool(value) => Some(value),
        _ => return Err(invalid_data("value is not boolean")),
    })
}

fn u8_field(fields: &[Value], index: usize) -> io::Result<Option<u8>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Ubyte(value) => Some(value),
        _ => return Err(invalid_data("value is not ubyte")),
    })
}

fn u16_field(fields: &[Value], index: usize) -> io::Result<Option<u16>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Ushort(value) => Some(value),
        _ => return Err(invalid_data("value is not ushort")),
    })
}

fn u32_field(fields: &[Value], index: usize) -> io::Result<Option<u32>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Uint(value) => Some(value),
        _ => return Err(invalid_data("value is not uint")),
    })
}

fn u64_field(fields: &[Value], index: usize) -> io::Result<Option<u64>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Ulong(value) => Some(value),
        _ => return Err(invalid_data("value is not ulong")),
    })
}

fn i64_field(fields: &[Value], index: usize) -> io::Result<Option<i64>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        Value::Long(value) => Some(value),
        Value::Timestamp(value) => Some(value.milliseconds()),
        _ => return Err(invalid_data("value is not long or timestamp")),
    })
}

fn required_u32(fields: &[Value], index: usize, name: &str) -> io::Result<u32> {
    u32_field(fields, index)?.ok_or_else(|| invalid_data(format!("{name} is required")))
}

fn required_u64(fields: &[Value], index: usize, name: &str) -> io::Result<u64> {
    u64_field(fields, index)?.ok_or_else(|| invalid_data(format!("{name} is required")))
}

fn error_field(fields: &[Value], index: usize) -> io::Result<Option<Error>> {
    Ok(match field(fields, index) {
        Value::Null => None,
        value => Some(error_from_value(value)?),
    })
}

fn optional_string(value: &Option<String>) -> Value {
    value
        .as_ref()
        .map(|value| Value::String(value.clone()))
        .unwrap_or(Value::Null)
}

fn optional_u32(value: Option<u32>) -> Value {
    value.map(Value::Uint).unwrap_or(Value::Null)
}

#[cfg(test)]
fn append_value(buffer: &mut Vec<u8>, value: Value) -> io::Result<()> {
    buffer.extend(encode_value(&value)?);
    Ok(())
}

fn encode_value(value: &Value) -> io::Result<Vec<u8>> {
    match value {
        Value::List(values) => encode_collection(values.iter(), values.len(), false),
        Value::Map(entries) => encode_collection(
            entries.iter().flat_map(|(key, value)| [key, value]),
            entries
                .len()
                .checked_mul(2)
                .ok_or_else(|| invalid_data("map size overflow"))?,
            true,
        ),
        Value::Array(values) => {
            let (constructor, payload) = array_parts(values)?;
            let mut body = constructor;
            body.extend(payload);
            encode_counted(0xe0, 0xf0, values.len(), body)
        }
        Value::Described(value) => {
            let mut encoded = vec![0x00];
            encoded.extend(encode_descriptor(&value.descriptor)?);
            encoded.extend(encode_value(&value.value)?);
            Ok(encoded)
        }
        Value::Null => Ok(vec![0x40]),
        Value::Bool(value) => Ok(vec![if *value { 0x41 } else { 0x42 }]),
        Value::Uint(0) => Ok(vec![0x43]),
        Value::Ulong(0) => Ok(vec![0x44]),
        Value::Uint(value) if *value <= u32::from(u8::MAX) => Ok(vec![0x52, *value as u8]),
        Value::Ulong(value) if *value <= u64::from(u8::MAX) => Ok(vec![0x53, *value as u8]),
        Value::Int(value) if i8::try_from(*value).is_ok() => Ok(vec![0x54, *value as u8]),
        Value::Long(value) if i8::try_from(*value).is_ok() => Ok(vec![0x55, *value as u8]),
        Value::Binary(value) => encode_variable(0xa0, 0xb0, value),
        Value::String(value) => encode_variable(0xa1, 0xb1, value.as_bytes()),
        Value::Symbol(value) => {
            if !value.as_str().is_ascii() {
                return Err(invalid_data("AMQP symbol contains non-ASCII characters"));
            }
            encode_variable(0xa3, 0xb3, value.as_str().as_bytes())
        }
        // Scalars use the same fixed payload as an array element, but carry
        // their own constructor when they are not inside an array.
        _ => {
            let (mut constructor, payload) = array_element(value)?;
            constructor.extend(payload);
            Ok(constructor)
        }
    }
}

fn encode_descriptor(descriptor: &Descriptor) -> io::Result<Vec<u8>> {
    match descriptor {
        Descriptor::Code(code) => encode_value(&Value::Ulong(*code)),
        Descriptor::Name(name) => encode_value(&Value::Symbol(name.clone())),
    }
}

fn encode_collection<'a>(
    values: impl Iterator<Item = &'a Value>,
    count: usize,
    map: bool,
) -> io::Result<Vec<u8>> {
    if !map && count == 0 {
        return Ok(vec![0x45]);
    }
    let mut body = Vec::new();
    for value in values {
        body.extend(encode_value(value)?);
    }
    encode_counted(
        if map { 0xc1 } else { 0xc0 },
        if map { 0xd1 } else { 0xd0 },
        count,
        body,
    )
}

fn encode_counted(short: u8, long: u8, count: usize, body: Vec<u8>) -> io::Result<Vec<u8>> {
    if let (Ok(size), Ok(count)) = (
        u8::try_from(body.len().saturating_add(1)),
        u8::try_from(count),
    ) {
        let mut encoded = vec![short, size, count];
        encoded.extend(body);
        return Ok(encoded);
    }
    let mut encoded = vec![long];
    encoded.extend(counted_payload(count, body)?);
    Ok(encoded)
}

fn counted_payload(count: usize, body: Vec<u8>) -> io::Result<Vec<u8>> {
    let size = u32::try_from(body.len().saturating_add(4))
        .map_err(|_| invalid_data("collection size overflow"))?;
    let count = u32::try_from(count).map_err(|_| invalid_data("collection count overflow"))?;
    let mut payload = size.to_be_bytes().to_vec();
    payload.extend_from_slice(&count.to_be_bytes());
    payload.extend(body);
    Ok(payload)
}

fn array_parts(values: &Array<Value>) -> io::Result<(Vec<u8>, Vec<u8>)> {
    let Some(first) = values.first() else {
        return Err(invalid_data(
            "empty array has no retained element constructor",
        ));
    };
    let (constructor, mut payload) = array_element(first)?;
    for value in values.iter().skip(1) {
        let (next_constructor, next_payload) = array_element(value)?;
        if constructor != next_constructor {
            return Err(invalid_data(
                "array elements have incompatible constructors",
            ));
        }
        payload.extend(next_payload);
    }
    Ok((constructor, payload))
}

// Array elements share one constructor. Fixed-width encodings and 32-bit
// collection lengths keep that constructor independent of each element's value.
fn array_element(value: &Value) -> io::Result<(Vec<u8>, Vec<u8>)> {
    let (code, payload) = match value {
        Value::Null => (0x40, Vec::new()),
        Value::Bool(value) => (0x56, vec![u8::from(*value)]),
        Value::Ubyte(value) => (0x50, vec![*value]),
        Value::Ushort(value) => (0x60, value.to_be_bytes().to_vec()),
        Value::Uint(value) => (0x70, value.to_be_bytes().to_vec()),
        Value::Ulong(value) => (0x80, value.to_be_bytes().to_vec()),
        Value::Byte(value) => (0x51, value.to_be_bytes().to_vec()),
        Value::Short(value) => (0x61, value.to_be_bytes().to_vec()),
        Value::Int(value) => (0x71, value.to_be_bytes().to_vec()),
        Value::Long(value) => (0x81, value.to_be_bytes().to_vec()),
        Value::Float(value) => (0x72, value.0.to_bits().to_be_bytes().to_vec()),
        Value::Double(value) => (0x82, value.0.to_bits().to_be_bytes().to_vec()),
        Value::Decimal32(value) => (0x74, value.clone().into_inner().to_vec()),
        Value::Decimal64(value) => (0x84, value.clone().into_inner().to_vec()),
        Value::Decimal128(value) => (0x94, value.clone().into_inner().to_vec()),
        Value::Char(value) => (0x73, u32::from(*value).to_be_bytes().to_vec()),
        Value::Timestamp(value) => (0x83, value.milliseconds().to_be_bytes().to_vec()),
        Value::Uuid(value) => (0x98, value.as_ref().to_vec()),
        Value::Binary(value) => (0xb0, variable_payload(value)?),
        Value::String(value) => (0xb1, variable_payload(value.as_bytes())?),
        Value::Symbol(value) => {
            if !value.as_str().is_ascii() {
                return Err(invalid_data("AMQP symbol contains non-ASCII characters"));
            }
            (0xb3, variable_payload(value.as_str().as_bytes())?)
        }
        Value::List(values) => {
            let mut body = Vec::new();
            for value in values {
                body.extend(encode_value(value)?);
            }
            (0xd0, counted_payload(values.len(), body)?)
        }
        Value::Map(entries) => {
            let mut body = Vec::new();
            for (key, value) in entries {
                body.extend(encode_value(key)?);
                body.extend(encode_value(value)?);
            }
            let count = entries
                .len()
                .checked_mul(2)
                .ok_or_else(|| invalid_data("map size overflow"))?;
            (0xd1, counted_payload(count, body)?)
        }
        Value::Array(values) => {
            let (mut body, payload) = array_parts(values)?;
            body.extend(payload);
            (0xf0, counted_payload(values.len(), body)?)
        }
        Value::Described(value) => {
            let (base_constructor, payload) = array_element(&value.value)?;
            let mut constructor = vec![0x00];
            constructor.extend(encode_descriptor(&value.descriptor)?);
            constructor.extend(base_constructor);
            return Ok((constructor, payload));
        }
    };
    Ok((vec![code], payload))
}

fn variable_payload(bytes: &[u8]) -> io::Result<Vec<u8>> {
    let size = u32::try_from(bytes.len()).map_err(|_| invalid_data("value size overflow"))?;
    let mut payload = size.to_be_bytes().to_vec();
    payload.extend_from_slice(bytes);
    Ok(payload)
}

fn encode_variable(short: u8, long: u8, bytes: &[u8]) -> io::Result<Vec<u8>> {
    if let Ok(size) = u8::try_from(bytes.len()) {
        let mut encoded = vec![short, size];
        encoded.extend_from_slice(bytes);
        return Ok(encoded);
    }
    let mut encoded = vec![long];
    encoded.extend(variable_payload(bytes)?);
    Ok(encoded)
}

fn invalid_data(error: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.into())
}

#[cfg(test)]
mod decode_budget_tests;

#[cfg(test)]
mod encode_budget_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer_frame(payload: Vec<u8>) -> Frame {
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Transfer(Transfer {
                handle: 0,
                delivery_id: None,
                delivery_tag: None,
                message_format: None,
                settled: None,
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            })),
            payload,
        }
    }

    #[tokio::test]
    async fn negotiated_frame_limit_accepts_the_exact_boundary() {
        let frame = transfer_frame(Vec::new());
        let overhead = encode_frame(&frame).expect("frame encodes").len();
        let frame = transfer_frame(vec![0; 512 - overhead]);
        let encoded = encode_frame(&frame).expect("frame encodes");
        assert_eq!(encoded.len(), 512);
        assert_eq!(
            read_frame_with_max_size(&mut encoded.as_slice(), 512)
                .await
                .expect("exact limit is accepted"),
            frame
        );
    }

    #[tokio::test]
    async fn invalid_frame_sizes_are_rejected_from_the_prefix_without_a_body() {
        use tokio::io::AsyncWriteExt;

        for (size, maximum) in [
            (0, 512),
            (7, 512),
            (513, 512),
            (MAX_FRAME_SIZE as u32 + 1, u32::MAX),
            (u32::MAX, u32::MAX),
        ] {
            let (mut reader, mut writer) = tokio::io::duplex(4);
            writer
                .write_all(&u32::to_be_bytes(size))
                .await
                .expect("size prefix is written");
            let error = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                read_frame_with_max_size(&mut reader, maximum),
            )
            .await
            .expect("rejection does not wait for a body")
            .expect_err("size is refused");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            let detail = error
                .get_ref()
                .and_then(|cause| cause.downcast_ref::<FrameSizeError>())
                .expect("frame size errors remain identifiable by the driver");
            assert_eq!(detail.size, size);
            assert_eq!(detail.maximum, maximum.min(MAX_FRAME_SIZE as u32));
        }
    }

    #[tokio::test]
    async fn invalid_frame_limits_do_not_consume_the_input() {
        for maximum in [0, 7] {
            let bytes = [0, 0, 0, 8, 2, 0, 0, 0];
            let mut input = bytes.as_slice();
            let error = read_frame_with_max_size(&mut input, maximum)
                .await
                .expect_err("invalid limit is refused");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(input, bytes);
        }
    }

    #[tokio::test]
    async fn legacy_reader_keeps_its_global_limit() {
        let frame = transfer_frame(vec![0; 1_024]);
        let bytes = encode_frame(&frame).expect("frame encodes");
        assert_eq!(
            read_frame(&mut bytes.as_slice())
                .await
                .expect("legacy read"),
            frame
        );
        assert_eq!(
            read_frame_with_max_size(&mut bytes.as_slice(), 512)
                .await
                .expect_err("negotiated read refuses larger frames")
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    fn round_trip(performative: Performative) {
        let frame = Frame::Amqp {
            channel: 3,
            performative: Some(performative),
            payload: b"payload".to_vec(),
        };
        let encoded = encode_frame(&frame).expect("frame encodes");
        assert_eq!(decode_frame(&encoded).expect("frame decodes"), frame);
    }

    fn source_attach(source: Source) -> Attach {
        Attach {
            name: String::from("source-default"),
            handle: 7,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::Second,
            source: Some(source),
            target: Some(Target::new("orders").into()),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: Some(0),
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    #[test]
    fn source_default_outcomes_round_trip_through_attach() {
        let mut details = Fields::default();
        details.insert(
            Symbol::from("detail"),
            Value::String(String::from("retained")),
        );
        for default_outcome in [
            None,
            Some(DeliveryState::Accepted(Accepted)),
            Some(DeliveryState::Rejected(Rejected {
                error: Some(Error::new(
                    AmqpError::NotAllowed,
                    "explicit source rejection",
                    Some(details.clone()),
                )),
            })),
            Some(DeliveryState::Released(Released)),
            Some(DeliveryState::Modified(Modified {
                delivery_failed: Some(true),
                undeliverable_here: Some(false),
                message_annotations: Some(details),
            })),
        ] {
            let mut source = Source::new("orders");
            source.default_outcome = default_outcome;
            round_trip(Performative::Attach(Box::new(source_attach(source))));
        }
    }

    #[test]
    fn source_default_outcome_decode_rejects_nonterminal_received() {
        let mut fields = vec![Value::Null; 9];
        fields[0] = Value::String(String::from("orders"));
        fields[8] = delivery_state_to_value(&DeliveryState::Received {
            section_number: 1,
            section_offset: 42,
        })
        .expect("Received remains valid as a delivery state");
        let invalid_source = described(SOURCE, Value::List(fields));
        let error = source_from_value(invalid_source.clone())
            .expect_err("Source requires an outcome rather than a nonterminal state");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "source default outcome must be terminal");

        let performative = Performative::Attach(Box::new(source_attach(Source::new("orders"))));
        let (_, body) = take_described(performative_to_value(&performative).unwrap()).unwrap();
        let mut attach_fields = take_list(body).unwrap();
        attach_fields[5] = invalid_source;
        let body = encode_value(&described(ATTACH, Value::List(attach_fields))).unwrap();
        let mut bytes = encode_frame(&Frame::Amqp {
            channel: 0,
            performative: None,
            payload: Vec::new(),
        })
        .unwrap();
        bytes.extend(body);
        let size = u32::try_from(bytes.len()).unwrap();
        bytes[..4].copy_from_slice(&size.to_be_bytes());
        let error =
            decode_frame(&bytes).expect_err("malformed Attach source cannot reach admission");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "source default outcome must be terminal");
    }

    #[tokio::test]
    async fn invalid_source_default_is_refused_before_writing_and_allows_valid_retry() {
        let mut source = Source::new("orders");
        source.default_outcome = Some(DeliveryState::Received {
            section_number: 0,
            section_offset: 0,
        });
        let mut frame = Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Attach(Box::new(source_attach(source)))),
            payload: Vec::new(),
        };
        let mut bytes = Vec::new();
        assert_eq!(
            write_frame(&mut bytes, &frame)
                .await
                .expect_err("invalid source defaults cannot emit a frame")
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(bytes.is_empty());
        let Frame::Amqp {
            performative: Some(Performative::Attach(attach)),
            ..
        } = &mut frame
        else {
            unreachable!("Attach fixture");
        };
        attach.source.as_mut().expect("source").default_outcome =
            Some(DeliveryState::Released(Released));
        write_frame(&mut bytes, &frame)
            .await
            .expect("a terminal default remains encodable");
        assert_eq!(decode_frame(&bytes).expect("valid retry decodes"), frame);
    }

    #[test]
    fn transport_performatives_round_trip() {
        round_trip(Performative::Open(Open::new("container")));
        round_trip(Performative::Begin(Begin::default()));
        round_trip(Performative::Attach(Box::new(Attach {
            name: String::from("orders"),
            handle: 7,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::Second,
            source: Some(Source::new("orders")),
            target: Some(Target::new("client").into()),
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: None,
            max_message_size: Some(262_144),
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        })));
        round_trip(Performative::Transfer(Transfer {
            handle: 7,
            delivery_id: Some(11),
            delivery_tag: Some(Binary::from(vec![9; 16])),
            message_format: Some(0),
            settled: Some(false),
            more: false,
            rcv_settle_mode: None,
            state: None,
            resume: false,
            aborted: false,
            batchable: false,
        }));
        round_trip(Performative::Disposition(Disposition {
            role: Role::Receiver,
            first: 11,
            last: None,
            settled: false,
            state: Some(DeliveryState::Accepted(Accepted)),
            batchable: false,
        }));
    }

    #[test]
    fn message_sections_round_trip() {
        let mut application_properties = ApplicationProperties::default();
        application_properties.insert("status-code", 202_i32);
        application_properties.insert("status-description", String::from("Accepted"));
        let message = Message {
            header: Some(Header {
                ttl: Some(5000),
                ..Header::default()
            }),
            message_annotations: None,
            properties: Some(Properties {
                message_id: Some(MessageId::String(String::from("message-1"))),
                group_id: Some(String::from("cart-1")),
                ..Properties::default()
            }),
            application_properties: Some(application_properties),
            body: Body::Data(vec![
                Binary::from(b"one".to_vec()),
                Binary::from(b"two".to_vec()),
            ]),
            ..Message::default()
        };

        let encoded = encode_message(&message).expect("message encodes");
        assert_eq!(decode_message(&encoded).expect("message decodes"), message);
    }

    #[test]
    fn array_values_share_one_constructor_including_null_and_nested_arrays() {
        let nulls = Message {
            body: Body::Value(Value::Array(vec![Value::Null, Value::Null].into())),
            ..Message::default()
        };
        let wire = [0x00, 0x53, 0x77, 0xe0, 0x02, 0x02, 0x40];
        assert_eq!(encode_message(&nulls).expect("null array encodes"), wire);
        assert_eq!(decode_message(&wire).expect("null array decodes"), nulls);
        for (index, values) in [
            vec![Value::Uint(0), Value::Uint(u32::MAX)],
            vec![Value::Bool(true), Value::Bool(false)],
            vec![Value::String(String::new()), Value::String("x".repeat(300))],
            vec![Value::List(Vec::new()), Value::List(vec![Value::Null])],
            vec![
                Value::Array(vec![Value::Null, Value::Null].into()),
                Value::Array(vec![Value::Int(3)].into()),
            ],
            vec![described(123, Value::Null), described(123, Value::Null)],
            vec![
                described(123, Value::Long(-1)),
                described(123, Value::Long(i64::MAX)),
            ],
        ]
        .into_iter()
        .enumerate()
        {
            let message = Message {
                body: Body::Value(Value::List(vec![Value::Array(values.into())])),
                ..Message::default()
            };
            assert_eq!(
                decode_message(&encode_message(&message).expect("array encodes"))
                    .unwrap_or_else(|error| panic!("array case {index} fails to decode: {error}")),
                message
            );
        }
        let mixed = Message {
            body: Body::Value(Value::Array(
                vec![Value::Int(1), Value::String("wrong".to_owned())].into(),
            )),
            ..Message::default()
        };
        assert!(encode_message(&mixed).is_err());
    }

    #[test]
    fn annotations_and_footer_accept_symbol_and_ulong_keys() {
        let mut annotations = Annotations::new();
        annotations.insert(Symbol::from("producer"), Value::String(String::from("one")));
        annotations.insert(7_u64, Value::Binary(Binary::from(vec![1, 2])));
        let message = Message {
            delivery_annotations: Some(annotations.clone()),
            message_annotations: Some(annotations.clone()),
            body: Body::Value(Value::Null),
            footer: Some(annotations),
            ..Message::default()
        };
        assert_eq!(
            decode_message(&encode_message(&message).expect("message encodes"))
                .expect("annotations decode"),
            message,
        );
        // A ulong key (0x53) in a footer map is distinct from a symbol key.
        let wire = [0x00, 0x53, 0x78, 0xc1, 0x04, 0x02, 0x53, 0x07, 0x40];
        assert_eq!(
            decode_message(&wire)
                .expect("ulong annotation decodes")
                .footer
                .expect("footer")
                .get(7_u64),
            Some(&Value::Null),
        );
        for invalid_key in [Value::String(String::from("key")), Value::Uint(7)] {
            let mut map = OrderedMap::new();
            map.insert(invalid_key, Value::Null);
            let wire = serde_amqp::to_vec(&described(FOOTER, Value::Map(map)))
                .expect("invalid key fixture encodes");
            assert_eq!(
                decode_message(&wire)
                    .expect_err("invalid annotation key")
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn allocation_budgets_cover_all_body_sections() {
        let count = u32::try_from(crate::value_codec::MAX_VALUE_ELEMENTS / 2)
            .expect("node count fits in u32");
        let mut section = vec![0x00, 0x53, 0x76, 0xc0, 0x0b, 0x01, 0xf0];
        section.extend(5_u32.to_be_bytes());
        section.extend(count.to_be_bytes());
        section.push(0x40);
        assert!(decode_message(&section).is_ok());
        assert!(decode_message(&[section.as_slice(), section.as_slice()].concat()).is_err());
    }

    #[test]
    fn message_sections_reject_repetition_and_invalid_order() {
        let header = [0x00, 0x53, 0x70, 0x45];
        let properties = [0x00, 0x53, 0x73, 0x45];
        let annotations = [0x00, 0x53, 0x72, 0xc1, 0x01, 0x00];
        let application = [0x00, 0x53, 0x74, 0xc1, 0x01, 0x00];
        let data = [0x00, 0x53, 0x75, 0xa0, 0x00];
        let footer = [0x00, 0x53, 0x78, 0xc1, 0x01, 0x00];
        for section in [
            header.as_slice(),
            properties.as_slice(),
            annotations.as_slice(),
            application.as_slice(),
            footer.as_slice(),
        ] {
            let wire = [section, section].concat();
            assert_eq!(
                decode_message(&wire).expect_err("repeated section").kind(),
                io::ErrorKind::InvalidData
            );
        }
        for (first, second) in [
            (data.as_slice(), header.as_slice()),
            (properties.as_slice(), annotations.as_slice()),
            (application.as_slice(), properties.as_slice()),
            (footer.as_slice(), data.as_slice()),
        ] {
            assert_eq!(
                decode_message(&[first, second].concat())
                    .expect_err("out of order section")
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
        assert!(decode_message(&[0x00, 0x53, 0x79, 0x40]).is_err());
    }

    #[test]
    fn known_symbolic_section_descriptors_decode() {
        let wire = serde_amqp::to_vec(&Value::Described(Box::new(Described {
            descriptor: Descriptor::Name(Symbol::from("amqp:data:binary")),
            value: Value::Binary(Binary::from(vec![1, 2, 3])),
        })))
        .expect("named descriptor encodes");
        assert_eq!(
            decode_message(&wire).expect("named section decodes").body,
            Body::Data(vec![Binary::from(vec![1, 2, 3])])
        );
    }

    #[test]
    fn property_times_use_timestamp_wire_values() {
        let message = Message {
            properties: Some(Properties {
                absolute_expiry_time: Some(1000),
                creation_time: Some(-1),
                ..Properties::default()
            }),
            ..Message::default()
        };
        // Property slots eight and nine use timestamp (0x83), not long (0x81).
        let wire = [
            0x00, 0x53, 0x73, 0xc0, 0x1b, 0x0a, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40,
            0x83, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, 0x83, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
        ];

        assert_eq!(encode_message(&message).expect("message encodes"), wire);
        assert_eq!(
            decode_message(&wire).expect("wire timestamps decode"),
            message
        );
    }

    #[test]
    fn default_header_priority_matches_an_omitted_wire_priority() {
        let omitted_priority = [0x00, 0x53, 0x70, 0x45];
        let decoded = decode_message(&omitted_priority).expect("empty header decodes");
        assert_eq!(decoded.header, Some(Header::default()));
        assert_eq!(Header::default().priority, 4);

        let message = Message {
            header: Some(Header::default()),
            ..Message::default()
        };
        let explicit_priority = [
            0x00, 0x53, 0x70, 0xc0, 0x07, 0x05, 0x42, 0x50, 0x04, 0x40, 0x42, 0x43,
        ];
        assert_eq!(
            encode_message(&message).expect("default header encodes"),
            explicit_priority,
        );
    }

    #[test]
    fn sequence_body_preserves_each_wire_section_including_empty_sections() {
        let message = Message {
            body: Body::Sequence(vec![
                vec![Value::Int(7)],
                Vec::new(),
                vec![Value::String(String::from("last"))],
            ]),
            ..Message::default()
        };
        let wire = [
            0x00, 0x53, 0x76, 0xc0, 0x03, 0x01, 0x54, 0x07, 0x00, 0x53, 0x76, 0x45, 0x00, 0x53,
            0x76, 0xc0, 0x07, 0x01, 0xa1, 0x04, b'l', b'a', b's', b't',
        ];

        assert_eq!(
            decode_message(&wire).expect("sequence sections decode"),
            message
        );
        assert_eq!(
            encode_message(&message).expect("sequence sections encode"),
            wire
        );

        let empty_sections = [0x00, 0x53, 0x76, 0x45, 0x00, 0x53, 0x76, 0x45];
        assert_eq!(
            decode_message(&empty_sections)
                .expect("empty sequence sections decode")
                .body,
            Body::Sequence(vec![Vec::new(), Vec::new()]),
        );
    }

    #[test]
    fn wire_body_sections_cannot_mix_types_or_repeat_a_value() {
        let data = [0x00, 0x53, 0x75, 0xa0, 0x00];
        let sequence = [0x00, 0x53, 0x76, 0x45];
        let value = [0x00, 0x53, 0x77, 0x40];
        for (first, second) in [
            (data.as_slice(), sequence.as_slice()),
            (data.as_slice(), value.as_slice()),
            (sequence.as_slice(), data.as_slice()),
            (sequence.as_slice(), value.as_slice()),
            (value.as_slice(), data.as_slice()),
            (value.as_slice(), sequence.as_slice()),
            (value.as_slice(), value.as_slice()),
        ] {
            let wire = [first, second].concat();
            assert_eq!(
                decode_message(&wire)
                    .expect_err("mixed or repeated value bodies must fail")
                    .kind(),
                io::ErrorKind::InvalidData,
            );
        }

        assert_eq!(
            decode_message(&value)
                .expect("one null value body decodes")
                .body,
            Body::Value(Value::Null),
        );
        assert_eq!(
            decode_message(&[data.as_slice(), data.as_slice()].concat())
                .expect("repeated data sections decode")
                .body,
            Body::Data(vec![Binary::from(Vec::new()), Binary::from(Vec::new())]),
        );
    }

    #[test]
    fn sasl_performatives_round_trip() {
        let frame = Frame::Sasl(SaslPerformative::Mechanisms(SaslMechanisms {
            mechanisms: vec![Symbol::from("ANONYMOUS"), Symbol::from("PLAIN")],
        }));
        let encoded = encode_frame(&frame).expect("frame encodes");
        assert_eq!(decode_frame(&encoded).expect("frame decodes"), frame);
    }
}
