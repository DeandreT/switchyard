use std::io;

use serde_amqp::{
    Value,
    described::Described,
    descriptor::Descriptor,
    primitives::{Array, Binary, OrderedMap, Symbol},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::types::*;

pub const AMQP_PROTOCOL_ID: u8 = 0;
pub const SASL_PROTOCOL_ID: u8 = 3;
pub const AMQP_HEADER: [u8; 8] = *b"AMQP\x00\x01\x00\x00";
pub const SASL_HEADER: [u8; 8] = *b"AMQP\x03\x01\x00\x00";

const AMQP_FRAME_TYPE: u8 = 0;
const SASL_FRAME_TYPE: u8 = 1;
const FRAME_HEADER_SIZE: usize = 8;
const MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;

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

const HEADER: u64 = 0x70;
const PROPERTIES: u64 = 0x73;
const APPLICATION_PROPERTIES: u64 = 0x74;
const DATA: u64 = 0x75;
const AMQP_SEQUENCE: u64 = 0x76;
const AMQP_VALUE: u64 = 0x77;

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
    let mut size_bytes = [0_u8; 4];
    reader.read_exact(&mut size_bytes).await?;
    let size = u32::from_be_bytes(size_bytes) as usize;
    if !(FRAME_HEADER_SIZE..=MAX_FRAME_SIZE).contains(&size) {
        return Err(invalid_data(format!("invalid AMQP frame size {size}")));
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
        .map(|performative| serde_amqp::to_vec(&performative))
        .transpose()
        .map_err(amqp_codec_error)?
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
            let performative_len = encoded_value_len(body)?;
            let value =
                serde_amqp::from_slice(&body[..performative_len]).map_err(amqp_codec_error)?;
            Ok(Frame::Amqp {
                channel,
                performative: Some(performative_from_value(value)?),
                payload: body[performative_len..].to_vec(),
            })
        }
        SASL_FRAME_TYPE if channel == 0 && !body.is_empty() => {
            let performative_len = encoded_value_len(body)?;
            if performative_len != body.len() {
                return Err(invalid_data("SASL frame carries trailing payload"));
            }
            let value = serde_amqp::from_slice(body).map_err(amqp_codec_error)?;
            Ok(Frame::Sasl(sasl_from_value(value)?))
        }
        SASL_FRAME_TYPE => Err(invalid_data("invalid SASL frame")),
        frame_type => Err(invalid_data(format!(
            "unsupported AMQP frame type {frame_type}"
        ))),
    }
}

pub fn encode_message(message: &Message) -> io::Result<Vec<u8>> {
    let mut encoded = Vec::new();
    if let Some(header) = &message.header {
        append_value(&mut encoded, header_to_value(header))?;
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
        Body::Sequence(sequence) => {
            append_value(
                &mut encoded,
                described(AMQP_SEQUENCE, Value::List(sequence.clone())),
            )?;
        }
        Body::Value(value) => {
            append_value(&mut encoded, described(AMQP_VALUE, value.clone()))?;
        }
        Body::Empty => {}
    }
    Ok(encoded)
}

pub fn decode_message(encoded: &[u8]) -> io::Result<Message> {
    let mut message = Message::default();
    let mut offset = 0;
    let mut data = Vec::new();
    while offset < encoded.len() {
        let len = encoded_value_len(&encoded[offset..])?;
        let value =
            serde_amqp::from_slice(&encoded[offset..offset + len]).map_err(amqp_codec_error)?;
        offset += len;

        let (descriptor, value) = take_described(value)?;
        match descriptor {
            HEADER => message.header = Some(header_from_value(value)?),
            PROPERTIES => message.properties = Some(properties_from_value(value)?),
            APPLICATION_PROPERTIES => {
                message.application_properties = Some(application_properties_from_value(value)?);
            }
            DATA => match value {
                Value::Binary(section) => data.push(section),
                _ => return Err(invalid_data("data section is not binary")),
            },
            AMQP_SEQUENCE => {
                let Value::List(sequence) = value else {
                    return Err(invalid_data("AMQP sequence body is not a list"));
                };
                message.body = Body::Sequence(sequence);
            }
            AMQP_VALUE => message.body = Body::Value(value),
            _ => {}
        }
    }
    if !data.is_empty() {
        message.body = Body::Data(data);
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
                    .unwrap_or(Value::Null),
                attach
                    .target
                    .as_ref()
                    .map(target_to_value)
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
                value => Some(target_from_value(value)?),
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

fn source_to_value(source: &Source) -> Value {
    described(
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
            source
                .default_outcome
                .as_ref()
                .map(delivery_state_to_value)
                .transpose()
                .unwrap_or(None)
                .unwrap_or(Value::Null),
            symbol_array(&source.outcomes),
            symbol_array(&source.capabilities),
        ]),
    )
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
            value => Some(delivery_state_from_value(value)?),
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

fn target_from_value(value: Value) -> io::Result<Target> {
    let (descriptor, value) = take_described(value)?;
    if descriptor != TARGET {
        return Err(invalid_data("terminus is not an AMQP target"));
    }
    let fields = take_list(value)?;
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
    })
}

fn delivery_state_from_value(value: Value) -> io::Result<DeliveryState> {
    let (descriptor, value) = take_described(value)?;
    let fields = take_list(value)?;
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
                .map(Value::Long)
                .unwrap_or(Value::Null),
            properties
                .creation_time
                .map(Value::Long)
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
    array.as_ref().map_or(Value::Null, |array| {
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
        Descriptor::Name(_) => return Err(invalid_data("symbolic descriptors are unsupported")),
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

fn append_value(buffer: &mut Vec<u8>, value: Value) -> io::Result<()> {
    buffer.extend(serde_amqp::to_vec(&value).map_err(amqp_codec_error)?);
    Ok(())
}

fn encoded_value_len(bytes: &[u8]) -> io::Result<usize> {
    let Some(code) = bytes.first().copied() else {
        return Err(invalid_data("missing AMQP value"));
    };
    let len = match code {
        0x00 => {
            let descriptor = encoded_value_len(
                bytes
                    .get(1..)
                    .ok_or_else(|| invalid_data("missing AMQP descriptor"))?,
            )?;
            let value_start = 1 + descriptor;
            value_start
                + encoded_value_len(
                    bytes
                        .get(value_start..)
                        .ok_or_else(|| invalid_data("missing described AMQP value"))?,
                )?
        }
        0x40..=0x45 => 1,
        0x50..=0x56 => 2,
        0x60..=0x61 => 3,
        0x70..=0x74 => 5,
        0x80..=0x84 => 9,
        0x94 | 0x98 => 17,
        0xa0 | 0xa1 | 0xa3 => {
            2 + usize::from(
                *bytes
                    .get(1)
                    .ok_or_else(|| invalid_data("missing AMQP value length"))?,
            )
        }
        0xb0 | 0xb1 | 0xb3 => {
            5 + u32::from_be_bytes(
                bytes
                    .get(1..5)
                    .ok_or_else(|| invalid_data("missing AMQP value length"))?
                    .try_into()
                    .map_err(|_| invalid_data("invalid AMQP value length"))?,
            ) as usize
        }
        0xc0 | 0xc1 | 0xe0 => {
            2 + usize::from(
                *bytes
                    .get(1)
                    .ok_or_else(|| invalid_data("missing AMQP compound length"))?,
            )
        }
        0xd0 | 0xd1 | 0xf0 => {
            5 + u32::from_be_bytes(
                bytes
                    .get(1..5)
                    .ok_or_else(|| invalid_data("missing AMQP compound length"))?
                    .try_into()
                    .map_err(|_| invalid_data("invalid AMQP compound length"))?,
            ) as usize
        }
        _ => return Err(invalid_data(format!("unknown AMQP format code {code:#x}"))),
    };
    if len > bytes.len() {
        return Err(invalid_data("AMQP value is truncated"));
    }
    Ok(len)
}

fn amqp_codec_error(error: serde_amqp::Error) -> io::Error {
    invalid_data(error.to_string())
}

fn invalid_data(error: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(performative: Performative) {
        let frame = Frame::Amqp {
            channel: 3,
            performative: Some(performative),
            payload: b"payload".to_vec(),
        };
        let encoded = encode_frame(&frame).expect("frame encodes");
        assert_eq!(decode_frame(&encoded).expect("frame decodes"), frame);
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
            target: Some(Target::new("client")),
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
        };

        let encoded = encode_message(&message).expect("message encodes");
        assert_eq!(decode_message(&encoded).expect("message decodes"), message);
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
