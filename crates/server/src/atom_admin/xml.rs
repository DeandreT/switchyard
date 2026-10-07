use std::fmt;

use domain::{
    EntityBinding, EntityIncarnationKind, FiniteQueueCapacity, QueueCapacityStatus,
    QueueCapacityView, QueueConfig,
};

mod decode;
mod duration;
mod encode;
mod lexical;

pub(crate) use decode::decode_definition;
pub(crate) use encode::{encode_entry, encode_error, encode_feed};

pub(crate) const MAX_BODY_BYTES: usize = 65_536;
pub(crate) const QUEUE_COLLECTION_PATH: &str = "$Resources/queues";
const MAX_DEPTH: usize = 16;
const MAX_EVENTS: usize = 2_048;
const MAX_ATTRIBUTES: usize = 32;
const MAX_NAMESPACE_BINDINGS: usize = 64;
const MAX_PROPERTIES: usize = 128;
const MAX_REPLY_BYTES: usize = 1_048_576;
const MAX_FEED_ENTRIES: usize = 100;
const MIB: u64 = 1_048_576;
const KIB: usize = 1_024;
const ATOM_NS: &str = "http://www.w3.org/2005/Atom";
const SERVICE_BUS_NS: &str = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
const XSI_NS: &str = "http://www.w3.org/2001/XMLSchema-instance";
const XML_NS: &str = "http://www.w3.org/XML/1998/namespace";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AtomQueueDefinition {
    pub(crate) config: QueueConfig,
    pub(crate) limit: FiniteQueueCapacity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AtomXmlError {
    Malformed,
    WorkLimitExceeded,
    InvalidDefinition,
    UnsupportedDefinition,
    ReplyLimitExceeded,
}

impl AtomXmlError {
    fn code(self) -> &'static str {
        match self {
            Self::Malformed => "InvalidXml",
            Self::WorkLimitExceeded => "XmlWorkLimitExceeded",
            Self::InvalidDefinition => "InvalidQueueDefinition",
            Self::UnsupportedDefinition => "UnsupportedQueueDefinition",
            Self::ReplyLimitExceeded => "XmlReplyLimitExceeded",
        }
    }

    fn detail(self) -> &'static str {
        match self {
            Self::Malformed => "The queue XML document is malformed.",
            Self::WorkLimitExceeded => "The queue XML document exceeds a work limit.",
            Self::InvalidDefinition => "The queue definition is invalid.",
            Self::UnsupportedDefinition => "The queue definition is not supported.",
            Self::ReplyLimitExceeded => "The queue XML response exceeds a work limit.",
        }
    }
}

impl fmt::Display for AtomXmlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.detail())
    }
}

impl std::error::Error for AtomXmlError {}

pub(crate) fn validate_view(view: &QueueCapacityView) -> Result<(), AtomXmlError> {
    let binding = &view.binding;
    EntityBinding::new(
        binding.namespace().clone(),
        binding.target().clone(),
        binding.owner().clone(),
        binding.kind(),
        binding.generation(),
    )
    .map_err(|_| AtomXmlError::UnsupportedDefinition)?;
    if binding.kind() != EntityIncarnationKind::Queue || binding.target() != binding.owner() {
        return Err(AtomXmlError::UnsupportedDefinition);
    }
    let path = binding.target().as_str();
    if path == QUEUE_COLLECTION_PATH
        || path.contains('\\')
        || path
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
        || !lexical::legal_chars(path)
    {
        return Err(AtomXmlError::UnsupportedDefinition);
    }
    let QueueCapacityStatus::FiniteV1 { limit, .. } = view.capacity else {
        return Err(AtomXmlError::UnsupportedDefinition);
    };
    validate_definition(AtomQueueDefinition {
        config: view.config,
        limit,
    })
    .map_err(|_| AtomXmlError::UnsupportedDefinition)
}

fn validate_definition(definition: AtomQueueDefinition) -> Result<(), AtomXmlError> {
    let config = definition.config;
    config
        .validate()
        .map_err(|_| AtomXmlError::InvalidDefinition)?;
    if config.requires_session || config.requires_duplicate_detection {
        return Err(AtomXmlError::UnsupportedDefinition);
    }
    if !(5_000..=300_000).contains(&config.lock_duration_millis)
        || config.max_delivery_count > i32::MAX as u32
        || !config.max_message_bytes.is_multiple_of(KIB)
        || !(1..=256).contains(&(config.max_message_bytes / KIB))
        || config
            .default_time_to_live_millis
            .is_some_and(|millis| !(1_000..=duration::MAX_DURATION_MILLIS).contains(&millis))
        || !definition.limit.bytes().is_multiple_of(MIB)
        || !(1..=i32::MAX as u64).contains(&(definition.limit.bytes() / MIB))
    {
        return Err(AtomXmlError::InvalidDefinition);
    }
    Ok(())
}

#[derive(Default)]
struct Budget {
    events: usize,
    properties: usize,
    decoded: usize,
}

fn bounded_add(current: &mut usize, amount: usize, maximum: usize) -> Result<(), AtomXmlError> {
    let next = current
        .checked_add(amount)
        .ok_or(AtomXmlError::WorkLimitExceeded)?;
    if next > maximum {
        return Err(AtomXmlError::WorkLimitExceeded);
    }
    *current = next;
    Ok(())
}

impl Budget {
    fn event(&mut self) -> Result<(), AtomXmlError> {
        bounded_add(&mut self.events, 1, MAX_EVENTS)
    }

    fn property(&mut self) -> Result<(), AtomXmlError> {
        bounded_add(&mut self.properties, 1, MAX_PROPERTIES)
    }

    fn decoded(&mut self, bytes: usize) -> Result<(), AtomXmlError> {
        bounded_add(&mut self.decoded, bytes, MAX_BODY_BYTES)
    }

    fn depth(depth: usize) -> Result<(), AtomXmlError> {
        if depth > MAX_DEPTH {
            return Err(AtomXmlError::WorkLimitExceeded);
        }
        Ok(())
    }

    fn attributes(count: usize) -> Result<(), AtomXmlError> {
        if count > MAX_ATTRIBUTES {
            return Err(AtomXmlError::WorkLimitExceeded);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
