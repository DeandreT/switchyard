use std::fmt;

use serde_amqp::{
    Value,
    primitives::{Array, Binary, OrderedMap, Symbol, Uuid},
};

pub type DeliveryTag = Binary;
pub type Fields = OrderedMap<Symbol, Value>;
pub type FilterSet = OrderedMap<Symbol, Value>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Role {
    Sender,
    Receiver,
}

impl Role {
    pub(crate) fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Bool(false) => Some(Self::Sender),
            Value::Bool(true) => Some(Self::Receiver),
            _ => None,
        }
    }

    pub(crate) fn to_value(&self) -> Value {
        Value::Bool(matches!(self, Self::Receiver))
    }

    pub fn opposite(&self) -> Self {
        match self {
            Self::Sender => Self::Receiver,
            Self::Receiver => Self::Sender,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum SenderSettleMode {
    Settled,
    Unsettled,
    #[default]
    Mixed,
}

impl SenderSettleMode {
    pub(crate) fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Ubyte(0) => Some(Self::Unsettled),
            Value::Ubyte(1) => Some(Self::Settled),
            Value::Ubyte(2) => Some(Self::Mixed),
            _ => None,
        }
    }

    pub(crate) fn to_value(&self) -> Value {
        Value::Ubyte(match self {
            Self::Unsettled => 0,
            Self::Settled => 1,
            Self::Mixed => 2,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ReceiverSettleMode {
    #[default]
    First,
    Second,
}

impl ReceiverSettleMode {
    pub(crate) fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Ubyte(0) => Some(Self::First),
            Value::Ubyte(1) => Some(Self::Second),
            _ => None,
        }
    }

    pub(crate) fn to_value(&self) -> Value {
        Value::Ubyte(match self {
            Self::First => 0,
            Self::Second => 1,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Source {
    pub address: Option<String>,
    pub durable: u32,
    pub expiry_policy: Option<Symbol>,
    pub timeout: u32,
    pub dynamic: bool,
    pub dynamic_node_properties: Option<Fields>,
    pub distribution_mode: Option<Symbol>,
    pub filter: Option<FilterSet>,
    pub default_outcome: Option<DeliveryState>,
    pub outcomes: Option<Array<Symbol>>,
    pub capabilities: Option<Array<Symbol>>,
}

impl Source {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: Some(address.into()),
            ..Self::default()
        }
    }

    pub fn builder() -> SourceBuilder {
        SourceBuilder(Source::default())
    }
}

pub struct SourceBuilder(Source);

impl SourceBuilder {
    pub fn address(mut self, address: impl Into<String>) -> Self {
        self.0.address = Some(address.into());
        self
    }

    pub fn filter(mut self, filter: FilterSet) -> Self {
        self.0.filter = Some(filter);
        self
    }

    pub fn build(self) -> Source {
        self.0
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Target {
    pub address: Option<String>,
    pub durable: u32,
    pub expiry_policy: Option<Symbol>,
    pub timeout: u32,
    pub dynamic: bool,
    pub dynamic_node_properties: Option<Fields>,
    pub capabilities: Option<Array<Symbol>>,
}

impl Target {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: Some(address.into()),
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Open {
    pub container_id: String,
    pub hostname: Option<String>,
    pub max_frame_size: u32,
    pub channel_max: u16,
    pub idle_time_out: Option<u32>,
    pub outgoing_locales: Option<Array<Symbol>>,
    pub incoming_locales: Option<Array<Symbol>>,
    pub offered_capabilities: Option<Array<Symbol>>,
    pub desired_capabilities: Option<Array<Symbol>>,
    pub properties: Option<Fields>,
}

impl Open {
    pub fn new(container_id: impl Into<String>) -> Self {
        Self {
            container_id: container_id.into(),
            hostname: None,
            max_frame_size: 262_144,
            channel_max: u16::MAX,
            idle_time_out: None,
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Begin {
    pub remote_channel: Option<u16>,
    pub next_outgoing_id: u32,
    pub incoming_window: u32,
    pub outgoing_window: u32,
    pub handle_max: u32,
    pub offered_capabilities: Option<Array<Symbol>>,
    pub desired_capabilities: Option<Array<Symbol>>,
    pub properties: Option<Fields>,
}

impl Default for Begin {
    fn default() -> Self {
        Self {
            remote_channel: None,
            next_outgoing_id: 0,
            incoming_window: 2048,
            outgoing_window: 2048,
            handle_max: u32::MAX,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Attach {
    pub name: String,
    pub handle: u32,
    pub role: Role,
    pub snd_settle_mode: SenderSettleMode,
    pub rcv_settle_mode: ReceiverSettleMode,
    pub source: Option<Source>,
    pub target: Option<Target>,
    pub unsettled: Option<OrderedMap<DeliveryTag, Option<DeliveryState>>>,
    pub incomplete_unsettled: bool,
    pub initial_delivery_count: Option<u32>,
    pub max_message_size: Option<u64>,
    pub offered_capabilities: Option<Array<Symbol>>,
    pub desired_capabilities: Option<Array<Symbol>>,
    pub properties: Option<Fields>,
}

impl Attach {
    pub fn response(&self, source: Option<Source>, target: Option<Target>) -> Self {
        Self {
            name: self.name.clone(),
            handle: self.handle,
            role: self.role.opposite(),
            snd_settle_mode: self.snd_settle_mode.clone(),
            rcv_settle_mode: self.rcv_settle_mode.clone(),
            source,
            target,
            unsettled: None,
            incomplete_unsettled: false,
            initial_delivery_count: (self.role == Role::Receiver).then_some(0),
            max_message_size: self.max_message_size,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Flow {
    pub next_incoming_id: Option<u32>,
    pub incoming_window: u32,
    pub next_outgoing_id: u32,
    pub outgoing_window: u32,
    pub handle: Option<u32>,
    pub delivery_count: Option<u32>,
    pub link_credit: Option<u32>,
    pub available: Option<u32>,
    pub drain: bool,
    pub echo: bool,
    pub properties: Option<Fields>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Transfer {
    pub handle: u32,
    pub delivery_id: Option<u32>,
    pub delivery_tag: Option<DeliveryTag>,
    pub message_format: Option<u32>,
    pub settled: Option<bool>,
    pub more: bool,
    pub rcv_settle_mode: Option<ReceiverSettleMode>,
    pub state: Option<DeliveryState>,
    pub resume: bool,
    pub aborted: bool,
    pub batchable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Disposition {
    pub role: Role,
    pub first: u32,
    pub last: Option<u32>,
    pub settled: bool,
    pub state: Option<DeliveryState>,
    pub batchable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Detach {
    pub handle: u32,
    pub closed: bool,
    pub error: Option<Error>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct End {
    pub error: Option<Error>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Close {
    pub error: Option<Error>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Performative {
    Open(Open),
    Begin(Begin),
    Attach(Box<Attach>),
    Flow(Flow),
    Transfer(Transfer),
    Disposition(Disposition),
    Detach(Detach),
    End(End),
    Close(Close),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorCondition {
    Amqp(AmqpError),
    Custom(Symbol),
}

impl ErrorCondition {
    pub fn as_symbol(&self) -> Symbol {
        match self {
            Self::Amqp(condition) => Symbol::from(condition.as_str()),
            Self::Custom(condition) => condition.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AmqpError {
    InternalError,
    NotFound,
    UnauthorizedAccess,
    DecodeError,
    ResourceLimitExceeded,
    NotAllowed,
    InvalidField,
    NotImplemented,
    ResourceLocked,
    PreconditionFailed,
    ResourceDeleted,
    IllegalState,
    FrameSizeTooSmall,
}

impl AmqpError {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InternalError => "amqp:internal-error",
            Self::NotFound => "amqp:not-found",
            Self::UnauthorizedAccess => "amqp:unauthorized-access",
            Self::DecodeError => "amqp:decode-error",
            Self::ResourceLimitExceeded => "amqp:resource-limit-exceeded",
            Self::NotAllowed => "amqp:not-allowed",
            Self::InvalidField => "amqp:invalid-field",
            Self::NotImplemented => "amqp:not-implemented",
            Self::ResourceLocked => "amqp:resource-locked",
            Self::PreconditionFailed => "amqp:precondition-failed",
            Self::ResourceDeleted => "amqp:resource-deleted",
            Self::IllegalState => "amqp:illegal-state",
            Self::FrameSizeTooSmall => "amqp:frame-size-too-small",
        }
    }

    pub fn from_symbol(symbol: &str) -> Option<Self> {
        Some(match symbol {
            "amqp:internal-error" => Self::InternalError,
            "amqp:not-found" => Self::NotFound,
            "amqp:unauthorized-access" => Self::UnauthorizedAccess,
            "amqp:decode-error" => Self::DecodeError,
            "amqp:resource-limit-exceeded" => Self::ResourceLimitExceeded,
            "amqp:not-allowed" => Self::NotAllowed,
            "amqp:invalid-field" => Self::InvalidField,
            "amqp:not-implemented" => Self::NotImplemented,
            "amqp:resource-locked" => Self::ResourceLocked,
            "amqp:precondition-failed" => Self::PreconditionFailed,
            "amqp:resource-deleted" => Self::ResourceDeleted,
            "amqp:illegal-state" => Self::IllegalState,
            "amqp:frame-size-too-small" => Self::FrameSizeTooSmall,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    pub condition: ErrorCondition,
    pub description: Option<String>,
    pub info: Option<Fields>,
}

impl Error {
    pub fn new(
        condition: impl Into<ErrorCondition>,
        description: impl Into<String>,
        info: Option<Fields>,
    ) -> Self {
        Self {
            condition: condition.into(),
            description: Some(description.into()),
            info,
        }
    }
}

impl From<AmqpError> for ErrorCondition {
    fn from(value: AmqpError) -> Self {
        Self::Amqp(value)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Accepted;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Released;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Modified {
    pub delivery_failed: Option<bool>,
    pub undeliverable_here: Option<bool>,
    pub message_annotations: Option<Fields>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Rejected {
    pub error: Option<Error>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryState {
    Received {
        section_number: u32,
        section_offset: u64,
    },
    Accepted(Accepted),
    Rejected(Rejected),
    Released(Released),
    Modified(Modified),
}

impl DeliveryState {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Received { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    Accepted(Accepted),
    Rejected(Rejected),
    Released(Released),
    Modified(Modified),
}

impl TryFrom<DeliveryState> for Outcome {
    type Error = DeliveryState;

    fn try_from(value: DeliveryState) -> Result<Self, Self::Error> {
        match value {
            DeliveryState::Accepted(value) => Ok(Self::Accepted(value)),
            DeliveryState::Rejected(value) => Ok(Self::Rejected(value)),
            DeliveryState::Released(value) => Ok(Self::Released(value)),
            DeliveryState::Modified(value) => Ok(Self::Modified(value)),
            other => Err(other),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MessageId {
    Ulong(u64),
    Uuid(Uuid),
    Binary(Binary),
    String(String),
}

impl From<String> for MessageId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for MessageId {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Header {
    pub durable: bool,
    pub priority: u8,
    pub ttl: Option<u32>,
    pub first_acquirer: bool,
    pub delivery_count: u32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Properties {
    pub message_id: Option<MessageId>,
    pub user_id: Option<Binary>,
    pub to: Option<String>,
    pub subject: Option<String>,
    pub reply_to: Option<String>,
    pub correlation_id: Option<MessageId>,
    pub content_type: Option<Symbol>,
    pub content_encoding: Option<Symbol>,
    pub absolute_expiry_time: Option<i64>,
    pub creation_time: Option<i64>,
    pub group_id: Option<String>,
    pub group_sequence: Option<u32>,
    pub reply_to_group_id: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ApplicationProperties(pub OrderedMap<String, Value>);

impl ApplicationProperties {
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }

    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<Value>) {
        self.0.insert(name.into(), value.into());
    }

    pub fn builder() -> ApplicationPropertiesBuilder {
        ApplicationPropertiesBuilder(Self::default())
    }
}

pub struct ApplicationPropertiesBuilder(ApplicationProperties);

impl ApplicationPropertiesBuilder {
    pub fn insert(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.0.insert(name, value);
        self
    }

    pub fn build(self) -> ApplicationProperties {
        self.0
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum Body {
    Data(Vec<Binary>),
    Sequence(Vec<Value>),
    Value(Value),
    #[default]
    Empty,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Message {
    pub header: Option<Header>,
    pub properties: Option<Properties>,
    pub application_properties: Option<ApplicationProperties>,
    pub body: Body,
}

impl Message {
    pub fn data(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            body: Body::Data(vec![Binary::from(bytes.into())]),
            ..Self::default()
        }
    }

    pub fn builder() -> MessageBuilder {
        MessageBuilder(Self::default())
    }
}

pub struct MessageBuilder(Message);

impl MessageBuilder {
    pub fn properties(mut self, properties: Properties) -> Self {
        self.0.properties = Some(properties);
        self
    }

    pub fn application_properties(mut self, properties: ApplicationProperties) -> Self {
        self.0.application_properties = Some(properties);
        self
    }

    pub fn body(mut self, body: Body) -> Self {
        self.0.body = body;
        self
    }

    pub fn build(self) -> Message {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SaslMechanisms {
    pub mechanisms: Vec<Symbol>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SaslInit {
    pub mechanism: Symbol,
    pub initial_response: Option<Binary>,
    pub hostname: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SaslChallenge {
    pub challenge: Binary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SaslResponse {
    pub response: Binary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SaslOutcome {
    pub code: SaslCode,
    pub additional_data: Option<Binary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SaslCode {
    Ok,
    Auth,
    Sys,
    SysPerm,
    SysTemp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SaslPerformative {
    Mechanisms(SaslMechanisms),
    Init(SaslInit),
    Challenge(SaslChallenge),
    Response(SaslResponse),
    Outcome(SaslOutcome),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.condition.as_symbol().as_str())?;
        if let Some(description) = &self.description {
            write!(formatter, ": {description}")?;
        }
        Ok(())
    }
}
