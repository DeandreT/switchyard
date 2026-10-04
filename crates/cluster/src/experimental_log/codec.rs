use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::{self, Write},
};

use domain::{CommittedSend, EntityPath, NamespaceName, QueueConfig, SessionId, Timestamp};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess, Visitor},
};

use super::types::*;

const ENTRY_HEADER: &[u8] = b"SWLE\x01";
#[cfg(test)]
const VOTE_HEADER: &[u8] = b"SWLV\x01";
const PROFILE_HEADER: &[u8] = b"SWLP\x01";
const PROGRESS_HEADER: &[u8] = b"SWLS\x01";
const MAX_CONFIGS: usize = 2;
const MAX_NODES: usize = 32;
const MAX_ADDRESS_BYTES: usize = 512;

#[derive(Serialize, Deserialize)]
struct IdV1 {
    term: u64,
    node_id: u64,
    index: u64,
}

impl IdV1 {
    fn from_id(id: LogId) -> Self {
        Self {
            term: id.leader_id.term,
            node_id: id.leader_id.node_id,
            index: id.index,
        }
    }
    fn into_id(self) -> LogId {
        LogId::new(
            openraft::CommittedLeaderId::new(self.term, self.node_id),
            self.index,
        )
    }
}

#[derive(Serialize, Deserialize)]
struct VoteV1 {
    term: u64,
    node_id: u64,
    committed: bool,
}

impl VoteV1 {
    fn from_vote(vote: LogVote) -> Self {
        Self {
            term: vote.leader_id.term,
            node_id: vote.leader_id.node_id,
            committed: vote.committed,
        }
    }
    fn into_vote(self) -> LogVote {
        if self.committed {
            LogVote::new_committed(self.term, self.node_id)
        } else {
            LogVote::new(self.term, self.node_id)
        }
    }
}

#[derive(Serialize, Deserialize)]
struct QueueConfigV1 {
    lock_duration_millis: u64,
    max_delivery_count: u32,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: u64,
    requires_session: bool,
    requires_duplicate_detection: bool,
    duplicate_detection_history_time_window_millis: u64,
    dead_lettering_on_message_expiration: bool,
}

impl QueueConfigV1 {
    fn from_config(config: QueueConfig) -> Result<Self, LogCodecError> {
        Ok(Self {
            lock_duration_millis: config.lock_duration_millis,
            max_delivery_count: config.max_delivery_count,
            default_time_to_live_millis: config.default_time_to_live_millis,
            max_message_bytes: u64::try_from(config.max_message_bytes)
                .map_err(|_| LogCodecError::SizeOverflow)?,
            requires_session: config.requires_session,
            requires_duplicate_detection: config.requires_duplicate_detection,
            duplicate_detection_history_time_window_millis: config
                .duplicate_detection_history_time_window_millis,
            dead_lettering_on_message_expiration: config.dead_lettering_on_message_expiration,
        })
    }
    fn into_config(self) -> Result<QueueConfig, LogCodecError> {
        Ok(QueueConfig {
            lock_duration_millis: self.lock_duration_millis,
            max_delivery_count: self.max_delivery_count,
            default_time_to_live_millis: self.default_time_to_live_millis,
            max_message_bytes: usize::try_from(self.max_message_bytes)
                .map_err(|_| LogCodecError::SizeOverflow)?,
            requires_session: self.requires_session,
            requires_duplicate_detection: self.requires_duplicate_detection,
            duplicate_detection_history_time_window_millis: self
                .duplicate_detection_history_time_window_millis,
            dead_lettering_on_message_expiration: self.dead_lettering_on_message_expiration,
        })
    }
}

// The same frozen schema can borrow from disk bytes or own values supplied by
// another serde format. Visitors enforce caps before allocating copies.
struct Text<'a, const N: usize>(Cow<'a, str>);
impl<const N: usize> Serialize for Text<'_, N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}
impl<'de: 'a, 'a, const N: usize> Deserialize<'de> for Text<'a, N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextVisitor<const N: usize>;
        impl<'de, const N: usize> Visitor<'de> for TextVisitor<N> {
            type Value = Text<'de, N>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a bounded string")
            }
            fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
                if value.len() > N {
                    return Err(E::custom("log string exceeds its bound"));
                }
                Ok(Text(Cow::Borrowed(value)))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value.len() > N {
                    return Err(E::custom("log string exceeds its bound"));
                }
                Ok(Text(Cow::Owned(value.to_owned())))
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                if value.len() > N {
                    return Err(E::custom("log string exceeds its bound"));
                }
                Ok(Text(Cow::Owned(value)))
            }
        }
        deserializer.deserialize_str(TextVisitor::<N>)
    }
}

struct Body<'a>(Cow<'a, [u8]>);
impl Serialize for Body<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}
impl<'de: 'a, 'a> Deserialize<'de> for Body<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BodyVisitor;
        impl<'de> Visitor<'de> for BodyVisitor {
            type Value = Body<'de>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a bounded byte body")
            }
            fn visit_borrowed_bytes<E: de::Error>(
                self,
                value: &'de [u8],
            ) -> Result<Self::Value, E> {
                if value.len() > MAX_LOG_BODY_BYTES {
                    return Err(E::custom("log body exceeds its bound"));
                }
                Ok(Body(Cow::Borrowed(value)))
            }
            fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                if value.len() > MAX_LOG_BODY_BYTES {
                    return Err(E::custom("log body exceeds its bound"));
                }
                Ok(Body(Cow::Owned(value.to_vec())))
            }
            fn visit_byte_buf<E: de::Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
                if value.len() > MAX_LOG_BODY_BYTES {
                    return Err(E::custom("log body exceeds its bound"));
                }
                Ok(Body(Cow::Owned(value)))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let hint = seq.size_hint().unwrap_or(0);
                if hint > MAX_LOG_BODY_BYTES {
                    return Err(de::Error::custom("log body exceeds its bound"));
                }
                let mut body = Vec::with_capacity(hint);
                while let Some(byte) = seq.next_element()? {
                    if body.len() == MAX_LOG_BODY_BYTES {
                        return Err(de::Error::custom("log body exceeds its bound"));
                    }
                    body.push(byte);
                }
                Ok(Body(Cow::Owned(body)))
            }
        }
        deserializer.deserialize_bytes(BodyVisitor)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum QueueV1<'a> {
    CreateQueue {
        #[serde(borrow)]
        namespace: Text<'a, 50>,
        #[serde(borrow)]
        entity: Text<'a, 260>,
        issued_at: u64,
        config: QueueConfigV1,
    },
    Send {
        #[serde(borrow)]
        namespace: Text<'a, 50>,
        #[serde(borrow)]
        entity: Text<'a, 260>,
        issued_at: u64,
        #[serde(borrow)]
        message_id: Text<'a, MAX_LOG_ENTRY_BYTES>,
        #[serde(borrow)]
        body: Body<'a>,
        time_to_live_millis: Option<u64>,
        #[serde(borrow)]
        session_id: Option<Text<'a, 128>>,
    },
}

fn identifier(value: &str, max: usize) -> Result<(), LogCodecError> {
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        Err(LogCodecError::InvalidIdentifier)
    } else {
        Ok(())
    }
}

impl<'a> QueueV1<'a> {
    fn from_command(command: &'a QueueLogCommand) -> Result<Self, LogCodecError> {
        Ok(match command.0.as_ref() {
            QueueLogKind::CreateQueue {
                namespace,
                entity,
                issued_at,
                config,
            } => Self::CreateQueue {
                namespace: Text(Cow::Borrowed(namespace.as_str())),
                entity: Text(Cow::Borrowed(entity.as_str())),
                issued_at: issued_at.as_millis(),
                config: QueueConfigV1::from_config(*config)?,
            },
            QueueLogKind::Send {
                namespace,
                entity,
                issued_at,
                message,
            } => Self::Send {
                namespace: Text(Cow::Borrowed(namespace.as_str())),
                entity: Text(Cow::Borrowed(entity.as_str())),
                issued_at: issued_at.as_millis(),
                message_id: Text(Cow::Borrowed(&message.message_id)),
                body: Body(Cow::Borrowed(&message.body)),
                time_to_live_millis: message.time_to_live_millis,
                session_id: message
                    .session_id
                    .as_ref()
                    .map(|id| Text(Cow::Borrowed(id.as_str()))),
            },
        })
    }
    fn validate(&self) -> Result<(), LogCodecError> {
        let (namespace, entity) = match self {
            Self::CreateQueue {
                namespace,
                entity,
                config,
                ..
            } => {
                usize::try_from(config.max_message_bytes)
                    .map_err(|_| LogCodecError::SizeOverflow)?;
                (namespace, entity)
            }
            Self::Send {
                namespace,
                entity,
                message_id,
                body,
                session_id,
                ..
            } => {
                limit(body.0.len(), MAX_LOG_BODY_BYTES, LogResource::Body)?;
                limit(message_id.0.len(), MAX_LOG_ENTRY_BYTES, LogResource::Entry)?;
                if let Some(id) = session_id {
                    identifier(&id.0, 128)?;
                }
                (namespace, entity)
            }
        };
        identifier(&namespace.0, 50)?;
        identifier(&entity.0, 260)?;
        // The domain fingerprint also carries stream, previous mark, and full
        // entry identity. Its worst-case frozen wrapper is 114 bytes.
        encoded_size(self, MAX_LOG_QUEUE_BYTES, LogResource::Entry)?;
        Ok(())
    }
    fn into_command(self) -> Result<QueueLogCommand, LogCodecError> {
        self.validate()?;
        Ok(match self {
            Self::CreateQueue {
                namespace,
                entity,
                issued_at,
                config,
            } => QueueLogCommand::create_queue(
                NamespaceName::new(namespace.0.into_owned())
                    .map_err(|_| LogCodecError::InvalidIdentifier)?,
                EntityPath::new(entity.0.into_owned())
                    .map_err(|_| LogCodecError::InvalidIdentifier)?,
                Timestamp::from_millis(issued_at),
                config.into_config()?,
            ),
            Self::Send {
                namespace,
                entity,
                issued_at,
                message_id,
                body,
                time_to_live_millis,
                session_id,
            } => QueueLogCommand::send(
                NamespaceName::new(namespace.0.into_owned())
                    .map_err(|_| LogCodecError::InvalidIdentifier)?,
                EntityPath::new(entity.0.into_owned())
                    .map_err(|_| LogCodecError::InvalidIdentifier)?,
                Timestamp::from_millis(issued_at),
                CommittedSend {
                    message_id: message_id.0.into_owned(),
                    body: body.0.into_owned(),
                    time_to_live_millis,
                    session_id: session_id
                        .map(|id| {
                            SessionId::new(id.0.into_owned())
                                .map_err(|_| LogCodecError::InvalidIdentifier)
                        })
                        .transpose()?,
                },
            ),
        })
    }
}

pub(super) fn serialize_queue<S: Serializer>(
    command: &QueueLogCommand,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let wire = QueueV1::from_command(command).map_err(serde::ser::Error::custom)?;
    wire.validate().map_err(serde::ser::Error::custom)?;
    wire.serialize(serializer)
}

pub(super) fn deserialize_queue<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<QueueLogCommand, D::Error> {
    let wire = QueueV1::deserialize(deserializer)?;
    wire.validate().map_err(de::Error::custom)?;
    wire.into_command().map_err(de::Error::custom)
}

#[derive(Serialize)]
struct Bounded<T, const N: usize>(Vec<T>);
impl<'de, T: Deserialize<'de>, const N: usize> Deserialize<'de> for Bounded<T, N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BoundedVisitor<T, const N: usize>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for BoundedVisitor<T, N> {
            type Value = Bounded<T, N>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a bounded sequence")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let hint = seq.size_hint().unwrap_or(0);
                if hint > N {
                    return Err(de::Error::custom("log sequence exceeds its bound"));
                }
                let mut values = Vec::with_capacity(hint);
                while let Some(value) = seq.next_element()? {
                    if values.len() == N {
                        return Err(de::Error::custom("log sequence exceeds its bound"));
                    }
                    values.push(value);
                }
                Ok(Bounded(values))
            }
        }
        deserializer.deserialize_seq(BoundedVisitor::<T, N>(std::marker::PhantomData))
    }
}

#[derive(Serialize, Deserialize)]
struct NodeV1<'a> {
    id: u64,
    #[serde(borrow)]
    address: Text<'a, MAX_ADDRESS_BYTES>,
}
#[derive(Serialize, Deserialize)]
struct MembershipV1<'a> {
    configs: Bounded<Bounded<u64, MAX_NODES>, MAX_CONFIGS>,
    #[serde(borrow)]
    nodes: Bounded<NodeV1<'a>, MAX_NODES>,
}

impl<'a> MembershipV1<'a> {
    fn from_membership(
        membership: &'a openraft::Membership<u64, openraft::BasicNode>,
    ) -> Result<Self, LogCodecError> {
        if membership.get_joint_config().len() > MAX_CONFIGS
            || membership.nodes().count() > MAX_NODES
        {
            return Err(LogCodecError::InvalidMembership);
        }
        let mut configs = Vec::new();
        for config in membership.get_joint_config() {
            if config.len() > MAX_NODES {
                return Err(LogCodecError::InvalidMembership);
            }
            configs.push(Bounded(config.iter().copied().collect()));
        }
        let wire = Self {
            configs: Bounded(configs),
            nodes: Bounded(
                membership
                    .nodes()
                    .map(|(id, node)| NodeV1 {
                        id: *id,
                        address: Text(Cow::Borrowed(&node.addr)),
                    })
                    .collect(),
            ),
        };
        wire.validate()?;
        Ok(wire)
    }
    fn validate(&self) -> Result<(), LogCodecError> {
        if self.configs.0.is_empty()
            || self.configs.0.len() > MAX_CONFIGS
            || self.nodes.0.len() > MAX_NODES
        {
            return Err(LogCodecError::InvalidMembership);
        }
        if !self.nodes.0.windows(2).all(|pair| pair[0].id < pair[1].id) {
            return Err(LogCodecError::InvalidMembership);
        }
        for node in &self.nodes.0 {
            if node.address.0.len() > MAX_ADDRESS_BYTES {
                return Err(LogCodecError::InvalidMembership);
            }
        }
        for config in &self.configs.0 {
            if config.0.is_empty()
                || config.0.len() > MAX_NODES
                || !config.0.windows(2).all(|pair| pair[0] < pair[1])
            {
                return Err(LogCodecError::InvalidMembership);
            }
            if config.0.iter().any(|id| {
                self.nodes
                    .0
                    .binary_search_by_key(id, |node| node.id)
                    .is_err()
            }) {
                return Err(LogCodecError::InvalidMembership);
            }
        }
        encoded_size(self, MAX_LOG_MEMBERSHIP_BYTES, LogResource::Membership)?;
        Ok(())
    }
    fn into_membership(self) -> openraft::Membership<u64, openraft::BasicNode> {
        let configs: Vec<BTreeSet<u64>> = self
            .configs
            .0
            .into_iter()
            .map(|config| config.0.into_iter().collect())
            .collect();
        let nodes: BTreeMap<u64, openraft::BasicNode> = self
            .nodes
            .0
            .into_iter()
            .map(|node| {
                (
                    node.id,
                    openraft::BasicNode {
                        addr: node.address.0.into_owned(),
                    },
                )
            })
            .collect();
        openraft::Membership::new(configs, nodes)
    }
}

#[derive(Serialize, Deserialize)]
enum PayloadV1<'a> {
    Blank,
    #[serde(borrow)]
    Normal(QueueV1<'a>),
    #[serde(borrow)]
    Membership(MembershipV1<'a>),
}
#[derive(Serialize, Deserialize)]
struct EntryV1<'a> {
    id: IdV1,
    #[serde(borrow)]
    payload: PayloadV1<'a>,
}
impl<'a> EntryV1<'a> {
    fn from_entry(entry: &'a LogEntry) -> Result<Self, LogCodecError> {
        Ok(Self {
            id: IdV1::from_id(entry.log_id),
            payload: match &entry.payload {
                openraft::EntryPayload::Blank => PayloadV1::Blank,
                openraft::EntryPayload::Normal(command) => {
                    PayloadV1::Normal(QueueV1::from_command(command)?)
                }
                openraft::EntryPayload::Membership(membership) => {
                    PayloadV1::Membership(MembershipV1::from_membership(membership)?)
                }
            },
        })
    }
    fn validate(&self) -> Result<(), LogCodecError> {
        match &self.payload {
            PayloadV1::Blank => Ok(()),
            PayloadV1::Normal(command) => command.validate(),
            PayloadV1::Membership(membership) => membership.validate(),
        }
    }
    fn into_entry(self) -> Result<LogEntry, LogCodecError> {
        Ok(LogEntry {
            log_id: self.id.into_id(),
            payload: match self.payload {
                PayloadV1::Blank => openraft::EntryPayload::Blank,
                PayloadV1::Normal(command) => {
                    openraft::EntryPayload::Normal(command.into_command()?)
                }
                PayloadV1::Membership(membership) => {
                    openraft::EntryPayload::Membership(membership.into_membership())
                }
            },
        })
    }
}

fn limit(size: usize, maximum: usize, resource: LogResource) -> Result<(), LogCodecError> {
    if size > maximum {
        Err(LogCodecError::TooLarge { resource, maximum })
    } else {
        Ok(())
    }
}

struct Sink {
    bytes: Vec<u8>,
    maximum: usize,
    overflow: bool,
}
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            self.overflow = true;
            return Err(io::Error::other("log bound exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Counter {
    length: usize,
    maximum: usize,
    overflow: bool,
}
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.length) {
            self.overflow = true;
            return Err(io::Error::other("log bound exceeded"));
        }
        self.length += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Comparison<'a> {
    expected: &'a [u8],
    position: usize,
}
impl Write for Comparison<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .position
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("log comparison overflow"))?;
        if self.expected.get(self.position..end) != Some(bytes) {
            return Err(io::Error::other("noncanonical log bytes"));
        }
        self.position = end;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encoded_size<T: Serialize>(
    value: &T,
    maximum: usize,
    resource: LogResource,
) -> Result<usize, LogCodecError> {
    let mut counter = Counter {
        length: 0,
        maximum,
        overflow: false,
    };
    if postcard::to_io(value, &mut counter).is_err() {
        return Err(if counter.overflow {
            LogCodecError::TooLarge { resource, maximum }
        } else {
            LogCodecError::Malformed
        });
    }
    Ok(counter.length)
}
fn encode<T: Serialize>(
    header: &[u8],
    value: &T,
    maximum: usize,
    resource: LogResource,
) -> Result<Vec<u8>, LogCodecError> {
    let mut sink = Sink {
        bytes: header.to_vec(),
        maximum,
        overflow: false,
    };
    if postcard::to_io(value, &mut sink).is_err() {
        return Err(if sink.overflow {
            LogCodecError::TooLarge { resource, maximum }
        } else {
            LogCodecError::Malformed
        });
    }
    Ok(sink.bytes)
}
fn decode<'a, T: Deserialize<'a> + Serialize>(
    header: &[u8],
    bytes: &'a [u8],
    maximum: usize,
    resource: LogResource,
) -> Result<T, LogCodecError> {
    limit(bytes.len(), maximum, resource)?;
    let payload = bytes
        .strip_prefix(header)
        .ok_or(LogCodecError::UnsupportedRecord)?;
    let (wire, trailing) =
        postcard::take_from_bytes::<T>(payload).map_err(|_| LogCodecError::Malformed)?;
    if !trailing.is_empty() {
        return Err(LogCodecError::NonCanonical);
    }
    let mut comparison = Comparison {
        expected: payload,
        position: 0,
    };
    if postcard::to_io(&wire, &mut comparison).is_err() || comparison.position != payload.len() {
        return Err(LogCodecError::NonCanonical);
    }
    Ok(wire)
}

pub(super) fn encode_entry(entry: &LogEntry) -> Result<EncodedEntry, LogCodecError> {
    let wire = EntryV1::from_entry(entry)?;
    wire.validate()?;
    let bytes = encode(ENTRY_HEADER, &wire, MAX_LOG_ENTRY_BYTES, LogResource::Entry)?;
    Ok(EncodedEntry::validated(entry.log_id, bytes))
}

pub(super) fn entry_len(entry: &LogEntry) -> Result<usize, LogCodecError> {
    let wire = EntryV1::from_entry(entry)?;
    wire.validate()?;
    Ok(ENTRY_HEADER.len()
        + encoded_size(
            &wire,
            MAX_LOG_ENTRY_BYTES - ENTRY_HEADER.len(),
            LogResource::Entry,
        )?)
}

pub(super) fn queue_entry_upper_bound(command: &QueueLogCommand) -> Result<usize, LogCodecError> {
    let mut queue = QueueV1::from_command(command)?;
    match &mut queue {
        QueueV1::CreateQueue { issued_at, .. } | QueueV1::Send { issued_at, .. } => {
            *issued_at = u64::MAX
        }
    }
    let wire = EntryV1 {
        id: IdV1 {
            term: u64::MAX,
            node_id: u64::MAX,
            index: u64::MAX,
        },
        payload: PayloadV1::Normal(queue),
    };
    wire.validate()?;
    Ok(ENTRY_HEADER.len()
        + encoded_size(
            &wire,
            MAX_LOG_ENTRY_BYTES - ENTRY_HEADER.len(),
            LogResource::Entry,
        )?)
}
pub(super) fn decode_entry(bytes: &[u8]) -> Result<LogEntry, LogCodecError> {
    let wire: EntryV1<'_> = decode(ENTRY_HEADER, bytes, MAX_LOG_ENTRY_BYTES, LogResource::Entry)?;
    wire.validate()?;
    wire.into_entry()
}
pub(super) fn validate_encoded_entry(bytes: Vec<u8>) -> Result<EncodedEntry, LogCodecError> {
    let wire: EntryV1<'_> = decode(
        ENTRY_HEADER,
        &bytes,
        MAX_LOG_ENTRY_BYTES,
        LogResource::Entry,
    )?;
    wire.validate()?;
    let id = wire.id.into_id();
    Ok(EncodedEntry::validated(id, bytes))
}

pub(super) fn encode_membership(
    membership: &openraft::Membership<u64, openraft::BasicNode>,
) -> Result<Vec<u8>, LogCodecError> {
    let wire = MembershipV1::from_membership(membership)?;
    encode(
        &[],
        &wire,
        MAX_LOG_MEMBERSHIP_BYTES,
        LogResource::Membership,
    )
}

pub(super) fn decode_membership(
    bytes: &[u8],
) -> Result<openraft::Membership<u64, openraft::BasicNode>, LogCodecError> {
    let wire: MembershipV1<'_> = decode(
        &[],
        bytes,
        MAX_LOG_MEMBERSHIP_BYTES,
        LogResource::Membership,
    )?;
    wire.validate()?;
    Ok(wire.into_membership())
}
#[cfg(test)]
fn encode_vote(vote: &LogVote) -> Result<Vec<u8>, LogCodecError> {
    encode(
        VOTE_HEADER,
        &VoteV1::from_vote(*vote),
        MAX_LOG_METADATA_BYTES,
        LogResource::Metadata,
    )
}
#[cfg(test)]
fn decode_vote(bytes: &[u8]) -> Result<LogVote, LogCodecError> {
    decode::<VoteV1>(
        VOTE_HEADER,
        bytes,
        MAX_LOG_METADATA_BYTES,
        LogResource::Metadata,
    )
    .map(VoteV1::into_vote)
}

#[derive(Serialize, Deserialize)]
struct ProfileV1<'a> {
    #[serde(borrow)]
    role: Text<'a, 32>,
    node_id: u64,
    stream: [u8; 16],
    codec_version: u16,
    leader_id_mode: u8,
    body_bytes: u64,
    entry_bytes: u64,
    queue_bytes: u64,
    membership_bytes: u64,
    metadata_bytes: u64,
    append_entries: u64,
    append_bytes: u64,
    retained_entries: u64,
    retained_bytes: u64,
    limited_entries: u64,
    limited_bytes: u64,
}
fn profile_wire(profile: &LogProfile) -> ProfileV1<'static> {
    ProfileV1 {
        role: Text(Cow::Borrowed("queue-log-only")),
        node_id: profile.node_id(),
        stream: *profile.stream().as_bytes(),
        codec_version: 1,
        leader_id_mode: 1,
        body_bytes: MAX_LOG_BODY_BYTES as u64,
        entry_bytes: MAX_LOG_ENTRY_BYTES as u64,
        queue_bytes: MAX_LOG_QUEUE_BYTES as u64,
        membership_bytes: MAX_LOG_MEMBERSHIP_BYTES as u64,
        metadata_bytes: MAX_LOG_METADATA_BYTES as u64,
        append_entries: MAX_APPEND_ENTRIES as u64,
        append_bytes: MAX_APPEND_BYTES as u64,
        retained_entries: MAX_RETAINED_ENTRIES,
        retained_bytes: MAX_RETAINED_BYTES,
        limited_entries: MAX_LIMITED_ENTRIES as u64,
        limited_bytes: MAX_LIMITED_BYTES as u64,
    }
}
pub(super) fn encode_profile(profile: &LogProfile) -> Result<Vec<u8>, LogCodecError> {
    profile.validate()?;
    encode(
        PROFILE_HEADER,
        &profile_wire(profile),
        MAX_LOG_METADATA_BYTES,
        LogResource::Metadata,
    )
}
pub(super) fn decode_profile(bytes: &[u8]) -> Result<LogProfile, LogCodecError> {
    let wire: ProfileV1<'_> = decode(
        PROFILE_HEADER,
        bytes,
        MAX_LOG_METADATA_BYTES,
        LogResource::Metadata,
    )?;
    let stream =
        domain::CommittedStreamId::new(wire.stream).map_err(|_| LogCodecError::InvalidProfile)?;
    let profile = LogProfile::new(wire.node_id, stream)?;
    if encode(
        PROFILE_HEADER,
        &profile_wire(&profile),
        MAX_LOG_METADATA_BYTES,
        LogResource::Metadata,
    )? != bytes
    {
        return Err(LogCodecError::InvalidProfile);
    }
    Ok(profile)
}

#[derive(Serialize, Deserialize)]
struct ProgressV1 {
    vote: Option<VoteV1>,
    last_purged: Option<IdV1>,
    last_present: Option<IdV1>,
    retained_entries: u64,
    retained_bytes: u64,
}
fn validate_progress(progress: &LogProgress) -> Result<(), LogCodecError> {
    if progress.retained_entries > MAX_RETAINED_ENTRIES
        || progress.retained_bytes > MAX_RETAINED_BYTES
    {
        return Err(LogCodecError::InvalidProgress);
    }
    match progress.last_present {
        None if progress.retained_entries == 0 && progress.retained_bytes == 0 => Ok(()),
        Some(last) if progress.retained_entries > 0 && progress.retained_bytes > 0 => {
            let count = match progress.last_purged {
                Some(purged) if purged.index < last.index && purged.leader_id <= last.leader_id => {
                    last.index.checked_sub(purged.index)
                }
                None => last.index.checked_add(1),
                _ => None,
            };
            if count == Some(progress.retained_entries) {
                Ok(())
            } else {
                Err(LogCodecError::InvalidProgress)
            }
        }
        _ => Err(LogCodecError::InvalidProgress),
    }
}
pub(super) fn encode_progress(progress: &LogProgress) -> Result<Vec<u8>, LogCodecError> {
    validate_progress(progress)?;
    let wire = ProgressV1 {
        vote: progress.vote.map(VoteV1::from_vote),
        last_purged: progress.last_purged.map(IdV1::from_id),
        last_present: progress.last_present.map(IdV1::from_id),
        retained_entries: progress.retained_entries,
        retained_bytes: progress.retained_bytes,
    };
    encode(
        PROGRESS_HEADER,
        &wire,
        MAX_LOG_METADATA_BYTES,
        LogResource::Metadata,
    )
}
pub(super) fn decode_progress(bytes: &[u8]) -> Result<LogProgress, LogCodecError> {
    let wire: ProgressV1 = decode(
        PROGRESS_HEADER,
        bytes,
        MAX_LOG_METADATA_BYTES,
        LogResource::Metadata,
    )?;
    let progress = LogProgress {
        vote: wire.vote.map(VoteV1::into_vote),
        last_purged: wire.last_purged.map(IdV1::into_id),
        last_present: wire.last_present.map(IdV1::into_id),
        retained_entries: wire.retained_entries,
        retained_bytes: wire.retained_bytes,
    };
    validate_progress(&progress)?;
    Ok(progress)
}

#[cfg(test)]
mod tests;
