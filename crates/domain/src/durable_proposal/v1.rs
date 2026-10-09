//! Do not reorder, extend or reuse these DTOs for a later proposal version.

use std::{collections::BTreeMap, fmt, marker::PhantomData};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{SeqAccess, Visitor},
};

use crate::{
    Command, CommandKind, CorrelationFilter, CorrelationValue, DEAD_LETTER_QUEUE_SUFFIX,
    EntityBinding, EntityBindingKind, EntityPath, FilterProperties, LockToken, MessageEnvelope,
    MessageInput, NamespaceName, QueueConfig, QueueConfigUpdate, QueueTimeToLiveUpdate,
    ReceiveMode, RuleFilter, RuleName, SequenceNumber, SessionHold, SessionId, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig, identifier::SUBSCRIPTION_PATH_SEGMENT,
};

use super::{DurableProposal, DurableProposalError as Error, MAX_DURABLE_PROPOSAL_BYTES};

#[derive(Serialize, Deserialize)]
pub(super) struct Proposal {
    authority: Authority,
    command: Instruction,
}

#[derive(Serialize, Deserialize)]
enum Authority {
    Unbound,
    Bound(Binding),
}
#[derive(Serialize, Deserialize)]
struct Binding {
    namespace: String,
    target: String,
    owner: String,
    kind: OwnerKind,
    generation: u64,
}
#[derive(Serialize, Deserialize)]
enum OwnerKind {
    Queue,
    Topic,
    Subscription,
}
#[derive(Serialize, Deserialize)]
struct Instruction {
    namespace: String,
    entity: String,
    issued_at: u64,
    kind: Kind,
}

// The ordinal tags are fixed at 0..=27 independently of CommandKind serde.
#[derive(Serialize, Deserialize)]
enum Kind {
    CreateQueue {
        config: Queue,
    },
    CreateTopic {
        config: Topic,
    },
    CreateSubscription {
        name: String,
        config: Subscription,
    },
    CreateRule {
        name: RuleIdentity,
        filter: Filter,
    },
    DeleteRule {
        name: RuleIdentity,
    },
    ListRules {
        skip: u32,
        max_rules: u32,
    },
    Send(Input),
    SendBatch {
        messages: Items<Input>,
    },
    CancelScheduled {
        sequences: Items<u64>,
    },
    Peek {
        from_sequence: u64,
        max_messages: u32,
        session: Option<Hold>,
    },
    Receive {
        mode: Mode,
        lock_duration_millis: Option<u64>,
        session: Option<Hold>,
    },
    Complete {
        sequence: u64,
        lock_token: u64,
    },
    Abandon {
        sequence: u64,
        lock_token: u64,
        replacement_envelope: Option<Box<Envelope>>,
    },
    Defer {
        sequence: u64,
        lock_token: u64,
        replacement_envelope: Option<Box<Envelope>>,
    },
    ReceiveDeferred {
        sequences: Items<u64>,
        mode: Mode,
        lock_duration_millis: Option<u64>,
        session: Option<Hold>,
    },
    DeadLetter {
        sequence: u64,
        lock_token: u64,
        reason: String,
        description: String,
        replacement_envelope: Option<Box<Envelope>>,
    },
    RenewLock {
        sequence: u64,
        lock_token: u64,
        lock_duration_millis: Option<u64>,
    },
    AcceptSession {
        session_id: Option<String>,
        lock_duration_millis: Option<u64>,
    },
    ReleaseSession {
        session: Hold,
    },
    RenewSessionLock {
        session: Hold,
        lock_duration_millis: Option<u64>,
    },
    SetSessionState {
        session: Hold,
        state: Bytes,
    },
    GetSessionState {
        session: Hold,
    },
    ExpireLocks,
    ExpireMessages,
    ExpireSessionLocks,
    ActivateScheduled,
    ExpireDuplicateHistory,
    UpdateQueue {
        update: QueueUpdate,
    },
}

#[derive(Serialize, Deserialize)]
struct Queue {
    lock_duration_millis: u64,
    max_delivery_count: u32,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: u64,
    requires_session: bool,
    requires_duplicate_detection: bool,
    duplicate_detection_history_millis: u64,
}
#[derive(Serialize, Deserialize)]
struct Topic {
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: u64,
}
#[derive(Serialize, Deserialize)]
struct Subscription {
    lock_duration_millis: u64,
    max_delivery_count: u32,
    default_time_to_live_millis: Option<u64>,
}
#[derive(Serialize, Deserialize)]
struct QueueUpdate {
    lock_duration_millis: Option<u64>,
    max_delivery_count: Option<u32>,
    default_time_to_live_millis: Option<Lifetime>,
    max_message_bytes: Option<u64>,
    requires_session: Option<bool>,
    requires_duplicate_detection: Option<bool>,
    duplicate_detection_history_millis: Option<u64>,
}
#[derive(Serialize, Deserialize)]
enum Lifetime {
    Finite { millis: u64 },
    Unlimited,
}
#[derive(Serialize, Deserialize)]
enum Mode {
    PeekLock,
    ReceiveAndDelete,
}
#[derive(Serialize, Deserialize)]
struct Hold {
    session_id: String,
    token: u64,
}
#[derive(Serialize, Deserialize)]
struct RuleIdentity {
    canonical: String,
    display: String,
}
#[derive(Serialize, Deserialize)]
enum Filter {
    True,
    False,
    Correlation(Box<Properties>),
}
#[derive(Serialize, Deserialize)]
struct Input {
    message_id: String,
    body: Bytes,
    time_to_live_millis: Option<u64>,
    session_id: Option<String>,
    scheduled_enqueue_at: Option<u64>,
    envelope: Option<Box<Envelope>>,
}
#[derive(Serialize, Deserialize)]
struct Envelope {
    bytes: Bytes,
    properties: Properties,
}
#[derive(Serialize, Deserialize)]
struct Properties {
    correlation_id: Option<String>,
    message_id: Option<String>,
    to: Option<String>,
    reply_to: Option<String>,
    subject: Option<String>,
    session_id: Option<String>,
    reply_to_session_id: Option<String>,
    content_type: Option<String>,
    application: Items<Property>,
}
#[derive(Serialize, Deserialize)]
struct Property {
    name: String,
    value: Bytes,
}

#[derive(Serialize)]
#[serde(transparent)]
struct Items<T>(Vec<T>);

// Ignore adversarial size hints: allocate only as concrete elements arrive.
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Items<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Elements<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for Elements<T> {
            type Value = Items<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("bounded V1 sequence")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    if values.len() >= MAX_DURABLE_PROPOSAL_BYTES {
                        return Err(serde::de::Error::custom(
                            "V1 sequence exceeds envelope bound",
                        ));
                    }
                    values.push(value);
                }
                Ok(Items(values))
            }
        }
        deserializer.deserialize_seq(Elements(PhantomData))
    }
}

struct Bytes(Vec<u8>);
impl Serialize for Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}
impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Buffer;
        impl<'de> Visitor<'de> for Buffer {
            type Value = Bytes;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("V1 bytes")
            }
            fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Bytes, E> {
                Ok(Bytes(value.to_vec()))
            }
        }
        deserializer.deserialize_byte_buf(Buffer)
    }
}

// Every variable-data copy and collection element charges a lower bound on its
// encoded bytes. Fixed DTO fields/container overhead is not part of this meter.
struct CopyBudget(usize);
impl CopyBudget {
    fn charge(&mut self, bytes: usize) -> Result<(), Error> {
        self.0 = self.0.checked_add(bytes).ok_or(Error::TooLarge)?;
        if self.0 > MAX_DURABLE_PROPOSAL_BYTES {
            return Err(Error::TooLarge);
        }
        Ok(())
    }
    fn string(&mut self, value: &str) -> Result<String, Error> {
        self.charge(value.len())?;
        Ok(value.to_owned())
    }
    fn bytes(&mut self, value: &[u8]) -> Result<Bytes, Error> {
        self.charge(value.len())?;
        Ok(Bytes(value.to_vec()))
    }
    fn optional(&mut self, value: &Option<String>) -> Result<Option<String>, Error> {
        value.as_deref().map(|value| self.string(value)).transpose()
    }
    fn session(&mut self, value: &Option<SessionId>) -> Result<Option<String>, Error> {
        value
            .as_ref()
            .map(|value| self.string(value.as_str()))
            .transpose()
    }
    fn hold(&mut self, value: &SessionHold) -> Result<Hold, Error> {
        Ok(Hold {
            session_id: self.string(value.session_id.as_str())?,
            token: value.token.as_u64(),
        })
    }
    fn optional_hold(&mut self, value: &Option<SessionHold>) -> Result<Option<Hold>, Error> {
        value.as_ref().map(|value| self.hold(value)).transpose()
    }
    fn rule(&mut self, value: &RuleName) -> Result<RuleIdentity, Error> {
        Ok(RuleIdentity {
            canonical: self.string(value.as_str())?,
            display: self.string(value.display_name())?,
        })
    }
    fn sequences(&mut self, values: &[SequenceNumber]) -> Result<Items<u64>, Error> {
        self.charge(values.len())?;
        Ok(Items(values.iter().map(|value| value.as_u64()).collect()))
    }
    fn application(
        &mut self,
        values: &BTreeMap<String, CorrelationValue>,
    ) -> Result<Items<Property>, Error> {
        let mut properties = Vec::new();
        for (name, value) in values {
            self.charge(1)?;
            properties.push(Property {
                name: self.string(name)?,
                value: self.bytes(value.as_bytes())?,
            });
        }
        Ok(Items(properties))
    }
    fn filter_properties(&mut self, value: &FilterProperties) -> Result<Properties, Error> {
        Ok(Properties {
            correlation_id: self.optional(&value.correlation_id)?,
            message_id: self.optional(&value.message_id)?,
            to: self.optional(&value.to)?,
            reply_to: self.optional(&value.reply_to)?,
            subject: self.optional(&value.subject)?,
            session_id: self.optional(&value.session_id)?,
            reply_to_session_id: self.optional(&value.reply_to_session_id)?,
            content_type: self.optional(&value.content_type)?,
            application: self.application(&value.application_properties)?,
        })
    }
    fn filter(&mut self, value: &RuleFilter) -> Result<Filter, Error> {
        Ok(match value {
            RuleFilter::True => Filter::True,
            RuleFilter::False => Filter::False,
            RuleFilter::Correlation(value) => Filter::Correlation(Box::new(Properties {
                correlation_id: self.optional(&value.correlation_id)?,
                message_id: self.optional(&value.message_id)?,
                to: self.optional(&value.to)?,
                reply_to: self.optional(&value.reply_to)?,
                subject: self.optional(&value.subject)?,
                session_id: self.optional(&value.session_id)?,
                reply_to_session_id: self.optional(&value.reply_to_session_id)?,
                content_type: self.optional(&value.content_type)?,
                application: self.application(&value.application_properties)?,
            })),
        })
    }
    fn envelope(
        &mut self,
        value: &Option<MessageEnvelope>,
    ) -> Result<Option<Box<Envelope>>, Error> {
        value
            .as_ref()
            .map(|value| {
                Ok(Box::new(Envelope {
                    bytes: self.bytes(value.as_bytes())?,
                    properties: self.filter_properties(value.filter_properties())?,
                }))
            })
            .transpose()
    }
    fn input(&mut self, value: &MessageInput) -> Result<Input, Error> {
        Ok(Input {
            message_id: self.string(&value.message_id)?,
            body: self.bytes(&value.body)?,
            time_to_live_millis: value.time_to_live_millis,
            session_id: self.session(&value.session_id)?,
            scheduled_enqueue_at: value.scheduled_enqueue_at.map(Timestamp::as_millis),
            envelope: self.envelope(&value.envelope)?,
        })
    }
}

impl Proposal {
    pub(super) fn capture(value: &DurableProposal) -> Result<Self, Error> {
        let mut budget = CopyBudget(0);
        let authority = match value.binding.as_ref() {
            None => Authority::Unbound,
            Some(binding) => {
                validate_binding(binding, &value.command)?;
                Authority::Bound(Binding {
                    namespace: budget.string(binding.namespace().as_str())?,
                    target: budget.string(binding.target().as_str())?,
                    owner: budget.string(binding.owner().as_str())?,
                    kind: match binding.kind() {
                        EntityBindingKind::Queue => OwnerKind::Queue,
                        EntityBindingKind::Topic => OwnerKind::Topic,
                        EntityBindingKind::Subscription => OwnerKind::Subscription,
                    },
                    generation: binding.generation(),
                })
            }
        };
        Ok(Self {
            authority,
            command: Instruction {
                namespace: budget.string(value.command.namespace.as_str())?,
                entity: budget.string(value.command.entity.as_str())?,
                issued_at: value.command.issued_at.as_millis(),
                kind: Kind::capture(&value.command.kind, &mut budget)?,
            },
        })
    }

    pub(super) fn restore(self) -> Result<DurableProposal, Error> {
        let command = Command::new(
            namespace(self.command.namespace)?,
            entity(self.command.entity)?,
            Timestamp::from_millis(self.command.issued_at),
            self.command.kind.restore()?,
        );
        let binding = match self.authority {
            Authority::Unbound => None,
            Authority::Bound(binding) => {
                let binding = EntityBinding::new(
                    namespace(binding.namespace)?,
                    entity(binding.target)?,
                    entity(binding.owner)?,
                    match binding.kind {
                        OwnerKind::Queue => EntityBindingKind::Queue,
                        OwnerKind::Topic => EntityBindingKind::Topic,
                        OwnerKind::Subscription => EntityBindingKind::Subscription,
                    },
                    binding.generation,
                );
                validate_binding(&binding, &command)?;
                Some(binding)
            }
        };
        Ok(DurableProposal { command, binding })
    }
}

impl Kind {
    fn capture(value: &CommandKind, b: &mut CopyBudget) -> Result<Self, Error> {
        Ok(match value {
            CommandKind::CreateQueue { config } => Self::CreateQueue {
                config: Queue::capture(*config)?,
            },
            CommandKind::CreateTopic { config } => Self::CreateTopic {
                config: Topic {
                    default_time_to_live_millis: config.default_time_to_live_millis,
                    max_message_bytes: size(config.max_message_bytes)?,
                },
            },
            CommandKind::CreateSubscription { name, config } => Self::CreateSubscription {
                name: b.string(name.as_str())?,
                config: Subscription {
                    lock_duration_millis: config.lock_duration_millis,
                    max_delivery_count: config.max_delivery_count,
                    default_time_to_live_millis: config.default_time_to_live_millis,
                },
            },
            CommandKind::CreateRule { name, filter } => Self::CreateRule {
                name: b.rule(name)?,
                filter: b.filter(filter)?,
            },
            CommandKind::DeleteRule { name } => Self::DeleteRule {
                name: b.rule(name)?,
            },
            CommandKind::ListRules { skip, max_rules } => Self::ListRules {
                skip: *skip,
                max_rules: *max_rules,
            },
            CommandKind::Send {
                message_id,
                body,
                time_to_live_millis,
                session_id,
                scheduled_enqueue_at,
                envelope,
            } => Self::Send(Input {
                message_id: b.string(message_id)?,
                body: b.bytes(body)?,
                time_to_live_millis: *time_to_live_millis,
                session_id: b.session(session_id)?,
                scheduled_enqueue_at: scheduled_enqueue_at.map(Timestamp::as_millis),
                envelope: b.envelope(envelope)?,
            }),
            CommandKind::SendBatch { messages } => {
                let mut inputs = Vec::new();
                for message in messages {
                    b.charge(1)?;
                    inputs.push(b.input(message)?);
                }
                Self::SendBatch {
                    messages: Items(inputs),
                }
            }
            CommandKind::CancelScheduled { sequences } => Self::CancelScheduled {
                sequences: b.sequences(sequences)?,
            },
            CommandKind::Peek {
                from_sequence,
                max_messages,
                session,
            } => Self::Peek {
                from_sequence: from_sequence.as_u64(),
                max_messages: *max_messages,
                session: b.optional_hold(session)?,
            },
            CommandKind::Receive {
                mode,
                lock_duration_millis,
                session,
            } => Self::Receive {
                mode: Mode::capture(*mode),
                lock_duration_millis: *lock_duration_millis,
                session: b.optional_hold(session)?,
            },
            CommandKind::Complete {
                sequence,
                lock_token,
            } => Self::Complete {
                sequence: sequence.as_u64(),
                lock_token: lock_token.as_u64(),
            },
            CommandKind::Abandon {
                sequence,
                lock_token,
                replacement_envelope,
            } => Self::Abandon {
                sequence: sequence.as_u64(),
                lock_token: lock_token.as_u64(),
                replacement_envelope: b.envelope(replacement_envelope)?,
            },
            CommandKind::Defer {
                sequence,
                lock_token,
                replacement_envelope,
            } => Self::Defer {
                sequence: sequence.as_u64(),
                lock_token: lock_token.as_u64(),
                replacement_envelope: b.envelope(replacement_envelope)?,
            },
            CommandKind::ReceiveDeferred {
                sequences,
                mode,
                lock_duration_millis,
                session,
            } => Self::ReceiveDeferred {
                sequences: b.sequences(sequences)?,
                mode: Mode::capture(*mode),
                lock_duration_millis: *lock_duration_millis,
                session: b.optional_hold(session)?,
            },
            CommandKind::DeadLetter {
                sequence,
                lock_token,
                reason,
                description,
                replacement_envelope,
            } => Self::DeadLetter {
                sequence: sequence.as_u64(),
                lock_token: lock_token.as_u64(),
                reason: b.string(reason)?,
                description: b.string(description)?,
                replacement_envelope: b.envelope(replacement_envelope)?,
            },
            CommandKind::RenewLock {
                sequence,
                lock_token,
                lock_duration_millis,
            } => Self::RenewLock {
                sequence: sequence.as_u64(),
                lock_token: lock_token.as_u64(),
                lock_duration_millis: *lock_duration_millis,
            },
            CommandKind::AcceptSession {
                session_id,
                lock_duration_millis,
            } => Self::AcceptSession {
                session_id: b.session(session_id)?,
                lock_duration_millis: *lock_duration_millis,
            },
            CommandKind::ReleaseSession { session } => Self::ReleaseSession {
                session: b.hold(session)?,
            },
            CommandKind::RenewSessionLock {
                session,
                lock_duration_millis,
            } => Self::RenewSessionLock {
                session: b.hold(session)?,
                lock_duration_millis: *lock_duration_millis,
            },
            CommandKind::SetSessionState { session, state } => Self::SetSessionState {
                session: b.hold(session)?,
                state: b.bytes(state)?,
            },
            CommandKind::GetSessionState { session } => Self::GetSessionState {
                session: b.hold(session)?,
            },
            CommandKind::ExpireLocks => Self::ExpireLocks,
            CommandKind::ExpireMessages => Self::ExpireMessages,
            CommandKind::ExpireSessionLocks => Self::ExpireSessionLocks,
            CommandKind::ActivateScheduled => Self::ActivateScheduled,
            CommandKind::ExpireDuplicateHistory => Self::ExpireDuplicateHistory,
            CommandKind::UpdateQueue { update } => Self::UpdateQueue {
                update: QueueUpdate::capture(*update)?,
            },
        })
    }

    fn restore(self) -> Result<CommandKind, Error> {
        Ok(match self {
            Self::CreateQueue { config } => CommandKind::CreateQueue {
                config: config.restore()?,
            },
            Self::CreateTopic { config } => CommandKind::CreateTopic {
                config: TopicConfig {
                    default_time_to_live_millis: config.default_time_to_live_millis,
                    max_message_bytes: native_size(config.max_message_bytes)?,
                },
            },
            Self::CreateSubscription { name, config } => CommandKind::CreateSubscription {
                name: subscription(name)?,
                config: SubscriptionConfig {
                    lock_duration_millis: config.lock_duration_millis,
                    max_delivery_count: config.max_delivery_count,
                    default_time_to_live_millis: config.default_time_to_live_millis,
                },
            },
            Self::CreateRule { name, filter } => CommandKind::CreateRule {
                name: name.restore()?,
                filter: filter.restore()?,
            },
            Self::DeleteRule { name } => CommandKind::DeleteRule {
                name: name.restore()?,
            },
            Self::ListRules { skip, max_rules } => CommandKind::ListRules { skip, max_rules },
            Self::Send(input) => {
                let input = input.restore()?;
                CommandKind::Send {
                    message_id: input.message_id,
                    body: input.body,
                    time_to_live_millis: input.time_to_live_millis,
                    session_id: input.session_id,
                    scheduled_enqueue_at: input.scheduled_enqueue_at,
                    envelope: input.envelope,
                }
            }
            Self::SendBatch { messages } => CommandKind::SendBatch {
                messages: messages
                    .0
                    .into_iter()
                    .map(Input::restore)
                    .collect::<Result<_, _>>()?,
            },
            Self::CancelScheduled { sequences } => CommandKind::CancelScheduled {
                sequences: sequences.0.into_iter().map(SequenceNumber::new).collect(),
            },
            Self::Peek {
                from_sequence,
                max_messages,
                session,
            } => CommandKind::Peek {
                from_sequence: SequenceNumber::new(from_sequence),
                max_messages,
                session: session.map(Hold::restore).transpose()?,
            },
            Self::Receive {
                mode,
                lock_duration_millis,
                session,
            } => CommandKind::Receive {
                mode: mode.restore(),
                lock_duration_millis,
                session: session.map(Hold::restore).transpose()?,
            },
            Self::Complete {
                sequence,
                lock_token,
            } => CommandKind::Complete {
                sequence: SequenceNumber::new(sequence),
                lock_token: LockToken::new(lock_token),
            },
            Self::Abandon {
                sequence,
                lock_token,
                replacement_envelope,
            } => CommandKind::Abandon {
                sequence: SequenceNumber::new(sequence),
                lock_token: LockToken::new(lock_token),
                replacement_envelope: restore_envelope(replacement_envelope)?,
            },
            Self::Defer {
                sequence,
                lock_token,
                replacement_envelope,
            } => CommandKind::Defer {
                sequence: SequenceNumber::new(sequence),
                lock_token: LockToken::new(lock_token),
                replacement_envelope: restore_envelope(replacement_envelope)?,
            },
            Self::ReceiveDeferred {
                sequences,
                mode,
                lock_duration_millis,
                session,
            } => CommandKind::ReceiveDeferred {
                sequences: sequences.0.into_iter().map(SequenceNumber::new).collect(),
                mode: mode.restore(),
                lock_duration_millis,
                session: session.map(Hold::restore).transpose()?,
            },
            Self::DeadLetter {
                sequence,
                lock_token,
                reason,
                description,
                replacement_envelope,
            } => CommandKind::DeadLetter {
                sequence: SequenceNumber::new(sequence),
                lock_token: LockToken::new(lock_token),
                reason,
                description,
                replacement_envelope: restore_envelope(replacement_envelope)?,
            },
            Self::RenewLock {
                sequence,
                lock_token,
                lock_duration_millis,
            } => CommandKind::RenewLock {
                sequence: SequenceNumber::new(sequence),
                lock_token: LockToken::new(lock_token),
                lock_duration_millis,
            },
            Self::AcceptSession {
                session_id,
                lock_duration_millis,
            } => CommandKind::AcceptSession {
                session_id: session_id.map(session).transpose()?,
                lock_duration_millis,
            },
            Self::ReleaseSession { session } => CommandKind::ReleaseSession {
                session: session.restore()?,
            },
            Self::RenewSessionLock {
                session,
                lock_duration_millis,
            } => CommandKind::RenewSessionLock {
                session: session.restore()?,
                lock_duration_millis,
            },
            Self::SetSessionState { session, state } => CommandKind::SetSessionState {
                session: session.restore()?,
                state: state.0,
            },
            Self::GetSessionState { session } => CommandKind::GetSessionState {
                session: session.restore()?,
            },
            Self::ExpireLocks => CommandKind::ExpireLocks,
            Self::ExpireMessages => CommandKind::ExpireMessages,
            Self::ExpireSessionLocks => CommandKind::ExpireSessionLocks,
            Self::ActivateScheduled => CommandKind::ActivateScheduled,
            Self::ExpireDuplicateHistory => CommandKind::ExpireDuplicateHistory,
            Self::UpdateQueue { update } => CommandKind::UpdateQueue {
                update: update.restore()?,
            },
        })
    }
}

impl Queue {
    fn capture(value: QueueConfig) -> Result<Self, Error> {
        Ok(Self {
            lock_duration_millis: value.lock_duration_millis,
            max_delivery_count: value.max_delivery_count,
            default_time_to_live_millis: value.default_time_to_live_millis,
            max_message_bytes: size(value.max_message_bytes)?,
            requires_session: value.requires_session,
            requires_duplicate_detection: value.requires_duplicate_detection,
            duplicate_detection_history_millis: value.duplicate_detection_history_millis,
        })
    }
    fn restore(self) -> Result<QueueConfig, Error> {
        Ok(QueueConfig {
            lock_duration_millis: self.lock_duration_millis,
            max_delivery_count: self.max_delivery_count,
            default_time_to_live_millis: self.default_time_to_live_millis,
            max_message_bytes: native_size(self.max_message_bytes)?,
            requires_session: self.requires_session,
            requires_duplicate_detection: self.requires_duplicate_detection,
            duplicate_detection_history_millis: self.duplicate_detection_history_millis,
        })
    }
}
impl QueueUpdate {
    fn capture(value: QueueConfigUpdate) -> Result<Self, Error> {
        Ok(Self {
            lock_duration_millis: value.lock_duration_millis,
            max_delivery_count: value.max_delivery_count,
            default_time_to_live_millis: value.default_time_to_live_millis.map(
                |value| match value {
                    QueueTimeToLiveUpdate::Finite { millis } => Lifetime::Finite { millis },
                    QueueTimeToLiveUpdate::Unlimited => Lifetime::Unlimited,
                },
            ),
            max_message_bytes: value.max_message_bytes.map(size).transpose()?,
            requires_session: value.requires_session,
            requires_duplicate_detection: value.requires_duplicate_detection,
            duplicate_detection_history_millis: value.duplicate_detection_history_millis,
        })
    }
    fn restore(self) -> Result<QueueConfigUpdate, Error> {
        Ok(QueueConfigUpdate {
            lock_duration_millis: self.lock_duration_millis,
            max_delivery_count: self.max_delivery_count,
            default_time_to_live_millis: self.default_time_to_live_millis.map(
                |value| match value {
                    Lifetime::Finite { millis } => QueueTimeToLiveUpdate::Finite { millis },
                    Lifetime::Unlimited => QueueTimeToLiveUpdate::Unlimited,
                },
            ),
            max_message_bytes: self.max_message_bytes.map(native_size).transpose()?,
            requires_session: self.requires_session,
            requires_duplicate_detection: self.requires_duplicate_detection,
            duplicate_detection_history_millis: self.duplicate_detection_history_millis,
        })
    }
}
impl Mode {
    fn capture(value: ReceiveMode) -> Self {
        match value {
            ReceiveMode::PeekLock => Self::PeekLock,
            ReceiveMode::ReceiveAndDelete => Self::ReceiveAndDelete,
        }
    }
    fn restore(self) -> ReceiveMode {
        match self {
            Self::PeekLock => ReceiveMode::PeekLock,
            Self::ReceiveAndDelete => ReceiveMode::ReceiveAndDelete,
        }
    }
}
impl Hold {
    fn restore(self) -> Result<SessionHold, Error> {
        Ok(SessionHold::new(
            session(self.session_id)?,
            LockToken::new(self.token),
        ))
    }
}
impl RuleIdentity {
    fn restore(self) -> Result<RuleName, Error> {
        let name = RuleName::new(self.display).map_err(|_| Error::InvalidIdentifier)?;
        if name.as_str() != self.canonical {
            return Err(Error::NonCanonical);
        }
        Ok(name)
    }
}
impl Input {
    fn restore(self) -> Result<MessageInput, Error> {
        Ok(MessageInput {
            message_id: self.message_id,
            body: self.body.0,
            time_to_live_millis: self.time_to_live_millis,
            session_id: self.session_id.map(session).transpose()?,
            scheduled_enqueue_at: self.scheduled_enqueue_at.map(Timestamp::from_millis),
            envelope: restore_envelope(self.envelope)?,
        })
    }
}
impl Properties {
    fn application(self) -> Result<FilterProperties, Error> {
        let mut application_properties = BTreeMap::new();
        for property in self.application.0 {
            if application_properties
                .last_key_value()
                .is_some_and(|(previous, _)| previous >= &property.name)
            {
                return Err(Error::NonCanonical);
            }
            application_properties.insert(
                property.name,
                CorrelationValue::from_durable_bytes(property.value.0),
            );
        }
        Ok(FilterProperties {
            correlation_id: self.correlation_id,
            message_id: self.message_id,
            to: self.to,
            reply_to: self.reply_to,
            subject: self.subject,
            session_id: self.session_id,
            reply_to_session_id: self.reply_to_session_id,
            content_type: self.content_type,
            application_properties,
        })
    }
}
impl Filter {
    fn restore(self) -> Result<RuleFilter, Error> {
        Ok(match self {
            Self::True => RuleFilter::True,
            Self::False => RuleFilter::False,
            Self::Correlation(properties) => {
                let p = properties.application()?;
                RuleFilter::Correlation(CorrelationFilter {
                    correlation_id: p.correlation_id,
                    message_id: p.message_id,
                    to: p.to,
                    reply_to: p.reply_to,
                    subject: p.subject,
                    session_id: p.session_id,
                    reply_to_session_id: p.reply_to_session_id,
                    content_type: p.content_type,
                    application_properties: p.application_properties,
                })
            }
        })
    }
}
fn restore_envelope(value: Option<Box<Envelope>>) -> Result<Option<MessageEnvelope>, Error> {
    value
        .map(|value| {
            Ok(MessageEnvelope::new(value.bytes.0)
                .with_filter_properties(value.properties.application()?))
        })
        .transpose()
}
fn size(value: usize) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error::IntegerOutOfRange)
}
fn native_size(value: u64) -> Result<usize, Error> {
    usize::try_from(value).map_err(|_| Error::IntegerOutOfRange)
}
fn namespace(value: String) -> Result<NamespaceName, Error> {
    canonical(&value)?;
    NamespaceName::new(value).map_err(|_| Error::InvalidIdentifier)
}
fn entity(value: String) -> Result<EntityPath, Error> {
    canonical(&value)?;
    EntityPath::from_internal(value).map_err(|_| Error::InvalidIdentifier)
}
fn subscription(value: String) -> Result<SubscriptionName, Error> {
    canonical(&value)?;
    SubscriptionName::new(value).map_err(|_| Error::InvalidIdentifier)
}
fn session(value: String) -> Result<SessionId, Error> {
    SessionId::new(value).map_err(|_| Error::InvalidIdentifier)
}
fn canonical(value: &str) -> Result<(), Error> {
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(Error::NonCanonical);
    }
    Ok(())
}
fn primary(value: &EntityPath) -> bool {
    EntityPath::new(value.as_str()).is_ok()
        && !value.is_dead_letter_queue()
        && !value.is_subscription()
        && !value.is_management()
}
pub(super) fn validate_binding(binding: &EntityBinding, command: &Command) -> Result<(), Error> {
    if binding.generation() == 0
        || binding.namespace() != &command.namespace
        || binding.target() != &command.entity
    {
        return Err(Error::InvalidAuthority);
    }
    let owner = binding.owner();
    let owner_valid = match binding.kind() {
        EntityBindingKind::Queue | EntityBindingKind::Topic => primary(owner),
        EntityBindingKind::Subscription => owner
            .as_str()
            .split_once(SUBSCRIPTION_PATH_SEGMENT)
            .is_some_and(|(topic, name)| {
                let Ok(topic) = EntityPath::new(topic) else {
                    return false;
                };
                let Ok(name) = SubscriptionName::new(name) else {
                    return false;
                };
                primary(&topic) && topic.subscription(&name).is_ok_and(|path| path == *owner)
            }),
    };
    if !owner_valid {
        return Err(Error::InvalidAuthority);
    }
    if binding.target() != owner
        && (binding.kind() == EntityBindingKind::Topic
            || binding
                .target()
                .as_str()
                .strip_suffix(DEAD_LETTER_QUEUE_SUFFIX)
                != Some(owner.as_str()))
    {
        return Err(Error::InvalidAuthority);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn original() -> Proposal {
        Proposal::capture(&DurableProposal::unbound(Command::new(
            NamespaceName::new("n").unwrap(),
            EntityPath::new("q").unwrap(),
            Timestamp::UNIX_EPOCH,
            CommandKind::ExpireLocks,
        )))
        .unwrap()
    }

    fn decode(value: &Proposal) -> Result<DurableProposal, Error> {
        DurableProposal::decode(&super::super::encode_v1(value).unwrap())
    }

    fn bound(kind: OwnerKind, owner: &str, target: &str, generation: u64) -> Proposal {
        let mut value = original();
        value.command.entity = target.to_owned();
        value.authority = Authority::Bound(Binding {
            namespace: "n".to_owned(),
            target: target.to_owned(),
            owner: owner.to_owned(),
            kind,
            generation,
        });
        value
    }

    #[test]
    fn forged_authority_shapes_and_scope_mismatches_refuse_without_rebinding() {
        for value in [
            bound(OwnerKind::Queue, "q", "q", 0),
            bound(OwnerKind::Queue, "q", "other", 1),
            bound(
                OwnerKind::Queue,
                "q/$deadletterqueue",
                "q/$deadletterqueue",
                1,
            ),
            bound(OwnerKind::Topic, "q", "q/$deadletterqueue", 1),
            bound(OwnerKind::Subscription, "q", "q", 1),
            bound(
                OwnerKind::Queue,
                "t/subscriptions/s",
                "t/subscriptions/s",
                1,
            ),
            bound(
                OwnerKind::Subscription,
                "t/subscriptions/s/subscriptions/x",
                "t/subscriptions/s/subscriptions/x",
                1,
            ),
            bound(
                OwnerKind::Subscription,
                "t/$management/subscriptions/s",
                "t/$management/subscriptions/s",
                1,
            ),
            bound(
                OwnerKind::Queue,
                "q",
                "q/$deadletterqueue/$deadletterqueue",
                1,
            ),
        ] {
            assert_eq!(decode(&value), Err(Error::InvalidAuthority));
        }
        for mismatch in [0, 1, 2] {
            let mut value = bound(OwnerKind::Queue, "q", "q", 1);
            if mismatch == 0 {
                value.command.namespace = "other".to_owned();
            } else if mismatch == 1 {
                value.command.entity = "other".to_owned();
            } else {
                let Authority::Bound(binding) = &mut value.authority else {
                    unreachable!()
                };
                binding.namespace = "other".to_owned();
            }
            assert_eq!(decode(&value), Err(Error::InvalidAuthority));
        }
        let topic = EntityPath::new("t".repeat(crate::MAX_ENTITY_PATH_BYTES)).unwrap();
        let owner = topic
            .subscription(
                &SubscriptionName::new("s".repeat(crate::MAX_SUBSCRIPTION_NAME_CHARACTERS))
                    .unwrap(),
            )
            .unwrap();
        let target = owner.dead_letter_queue().unwrap();
        for generation in [1, u64::MAX] {
            let value = bound(
                OwnerKind::Subscription,
                owner.as_str(),
                target.as_str(),
                generation,
            );
            let decoded = decode(&value).unwrap();
            let super::super::DurableProposalAuthority::Bound(binding) = decoded.authority() else {
                panic!("explicit bound authority")
            };
            assert_eq!(binding.generation(), generation);
            assert_eq!(binding.owner(), &owner);
            assert_eq!(binding.target(), &target);
        }
    }

    #[test]
    fn canonical_resource_names_and_rule_display_identity_are_checked() {
        for field in [0, 1] {
            for name in ["UPPER", "", "q\0x", "q\nx"] {
                let mut value = original();
                if field == 0 {
                    value.command.namespace = name.to_owned();
                } else {
                    value.command.entity = name.to_owned();
                }
                assert_eq!(
                    decode(&value),
                    Err(if name == "UPPER" {
                        Error::NonCanonical
                    } else {
                        Error::InvalidIdentifier
                    })
                );
            }
        }
        let mut value = original();
        value.command.kind = Kind::CreateSubscription {
            name: "Upper".to_owned(),
            config: Subscription {
                lock_duration_millis: 0,
                max_delivery_count: 0,
                default_time_to_live_millis: Some(0),
            },
        };
        assert_eq!(decode(&value), Err(Error::NonCanonical));
        value.command.kind = Kind::DeleteRule {
            name: RuleIdentity {
                canonical: "wrong".to_owned(),
                display: "$Default".to_owned(),
            },
        };
        assert_eq!(decode(&value), Err(Error::NonCanonical));
        value.command.kind = Kind::DeleteRule {
            name: RuleIdentity {
                canonical: "$default".to_owned(),
                display: "$DeFaUlT".to_owned(),
            },
        };
        let decoded = decode(&value).unwrap();
        let CommandKind::DeleteRule { name } = &decoded.command().kind else {
            unreachable!()
        };
        assert_eq!(name.display_name(), "$DeFaUlT");
        assert_eq!(name.as_str(), "$default");
        value.command.kind = Kind::ReleaseSession {
            session: Hold {
                session_id: "S\0x".to_owned(),
                token: 0,
            },
        };
        assert_eq!(decode(&value), Err(Error::InvalidIdentifier));
    }

    #[test]
    fn property_wire_order_and_exact_duplicates_are_not_silently_normalized() {
        fn value(names: &[&str]) -> Proposal {
            let filter = CorrelationFilter::default();
            let mut budget = CopyBudget(0);
            let Filter::Correlation(mut properties) =
                budget.filter(&RuleFilter::Correlation(filter)).unwrap()
            else {
                unreachable!()
            };
            properties.application = Items(
                names
                    .iter()
                    .map(|name| Property {
                        name: (*name).to_owned(),
                        value: Bytes(vec![0, 255]),
                    })
                    .collect(),
            );
            let mut value = original();
            value.command.kind = Kind::CreateRule {
                name: RuleIdentity {
                    canonical: "r".to_owned(),
                    display: "R".to_owned(),
                },
                filter: Filter::Correlation(properties),
            };
            value
        }
        for names in [&["b", "a"][..], &["a", "a"][..]] {
            assert_eq!(decode(&value(names)), Err(Error::NonCanonical));
        }
        let decoded = decode(&value(&["A", "a"])).unwrap();
        let CommandKind::CreateRule {
            filter: RuleFilter::Correlation(filter),
            ..
        } = &decoded.command().kind
        else {
            unreachable!()
        };
        assert_eq!(
            filter
                .application_properties
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["A", "a"]
        );
        assert!(RuleFilter::Correlation(filter.clone()).validate().is_err());
    }

    #[test]
    fn impossible_sequence_and_byte_lengths_fail_without_size_hint_allocation() {
        let mut raw = super::super::encode_v1(&original()).unwrap();
        // Replace the terminal kind with CancelScheduled and an impossible count.
        raw.truncate(18);
        raw.push(8);
        raw.extend_from_slice(&[255, 255, 255, 255, 255, 255, 255, 255, 255, 1]);
        let length = (raw.len() - 12) as u32;
        raw[8..12].copy_from_slice(&length.to_be_bytes());
        assert_eq!(DurableProposal::decode(&raw), Err(Error::Malformed));
        let mut raw = super::super::encode_v1(&original()).unwrap();
        // Namespace String length can never reach this declared value in a capped input.
        raw.splice(13..14, [255, 255, 255, 255, 255, 255, 255, 255, 255, 1]);
        let length = (raw.len() - 12) as u32;
        raw[8..12].copy_from_slice(&length.to_be_bytes());
        assert_eq!(DurableProposal::decode(&raw), Err(Error::Malformed));
        let mut value = original();
        value.command.kind = Kind::SetSessionState {
            session: Hold {
                session_id: "S".to_owned(),
                token: 0,
            },
            state: Bytes(Vec::new()),
        };
        let mut raw = super::super::encode_v1(&value).unwrap();
        // Replace the terminal Bytes length with u64::MAX, leaving no state bytes.
        assert_eq!(raw.pop(), Some(0));
        raw.extend_from_slice(&[255, 255, 255, 255, 255, 255, 255, 255, 255, 1]);
        let length = (raw.len() - 12) as u32;
        raw[8..12].copy_from_slice(&length.to_be_bytes());
        assert!(raw.len() <= MAX_DURABLE_PROPOSAL_BYTES);
        assert_eq!(DurableProposal::decode(&raw), Err(Error::Malformed));
    }

    #[test]
    fn private_nested_tags_cannot_be_extended_without_a_new_version() {
        for mutation in [0, 1, 2, 3] {
            let mut value = original();
            value.command.kind = Kind::Receive {
                mode: Mode::PeekLock,
                lock_duration_millis: None,
                session: None,
            };
            if mutation == 1 {
                value = bound(OwnerKind::Queue, "q", "q", 1);
            }
            if mutation == 2 {
                value.command.kind = Kind::CreateRule {
                    name: RuleIdentity {
                        canonical: "r".to_owned(),
                        display: "R".to_owned(),
                    },
                    filter: Filter::True,
                };
            }
            if mutation == 3 {
                value.command.kind = Kind::UpdateQueue {
                    update: QueueUpdate {
                        lock_duration_millis: None,
                        max_delivery_count: None,
                        default_time_to_live_millis: Some(Lifetime::Unlimited),
                        max_message_bytes: None,
                        requires_session: None,
                        requires_duplicate_detection: None,
                        duplicate_detection_history_millis: None,
                    },
                };
            }
            let mut raw = super::super::encode_v1(&value).unwrap();
            // Frozen offsets: mode, bound owner kind, filter, lifetime.
            let offset = [19, 19, 23, 22][mutation];
            raw[offset] = 127;
            assert_eq!(
                DurableProposal::decode(&raw),
                Err(Error::Malformed),
                "nested tag {mutation}"
            );
        }
    }

    #[test]
    fn copy_meter_refuses_overflow_and_zero_byte_collection_elements_are_charged() {
        let mut budget = CopyBudget(MAX_DURABLE_PROPOSAL_BYTES);
        budget.charge(0).unwrap();
        assert_eq!(budget.charge(1), Err(Error::TooLarge));
        assert!(budget.0 > MAX_DURABLE_PROPOSAL_BYTES);
        let mut overflow = CopyBudget(usize::MAX);
        assert_eq!(overflow.charge(1), Err(Error::TooLarge));
        let mut budget = CopyBudget(MAX_DURABLE_PROPOSAL_BYTES);
        let value = BTreeMap::from([(
            String::new(),
            CorrelationValue::from_durable_bytes(Vec::new()),
        )]);
        assert!(matches!(budget.application(&value), Err(Error::TooLarge)));
    }
}
