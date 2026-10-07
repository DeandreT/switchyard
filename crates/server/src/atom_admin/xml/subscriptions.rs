use std::fmt;

use domain::{SubscriptionConfig, SubscriptionName};

use super::{AtomXmlError, duration, lexical};

mod decode;
mod encode;

const MAX_MESSAGE_BYTES: usize = 262_144;

pub(crate) use decode::{decode_definition, decode_update_definition};
pub(crate) use encode::{encode_entry, encode_error};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SubscriptionXmlError {
    Malformed,
    WorkLimitExceeded,
    InvalidDefinition,
    UnsupportedDefinition,
    ReplyLimitExceeded,
}

impl From<AtomXmlError> for SubscriptionXmlError {
    fn from(error: AtomXmlError) -> Self {
        match error {
            AtomXmlError::Malformed => Self::Malformed,
            AtomXmlError::WorkLimitExceeded => Self::WorkLimitExceeded,
            AtomXmlError::InvalidDefinition => Self::InvalidDefinition,
            AtomXmlError::UnsupportedDefinition => Self::UnsupportedDefinition,
            AtomXmlError::ReplyLimitExceeded => Self::ReplyLimitExceeded,
        }
    }
}

impl SubscriptionXmlError {
    fn code(self) -> &'static str {
        match self {
            Self::Malformed => "InvalidXml",
            Self::WorkLimitExceeded => "XmlWorkLimitExceeded",
            Self::InvalidDefinition => "InvalidSubscriptionDefinition",
            Self::UnsupportedDefinition => "UnsupportedSubscriptionDefinition",
            Self::ReplyLimitExceeded => "XmlReplyLimitExceeded",
        }
    }

    fn detail(self) -> &'static str {
        match self {
            Self::Malformed => "The subscription XML document is malformed.",
            Self::WorkLimitExceeded => "The subscription XML document exceeds a work limit.",
            Self::InvalidDefinition => "The subscription definition is invalid.",
            Self::UnsupportedDefinition => "The subscription definition is not supported.",
            Self::ReplyLimitExceeded => "The subscription XML response exceeds a work limit.",
        }
    }
}

impl fmt::Display for SubscriptionXmlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.detail())
    }
}

impl std::error::Error for SubscriptionXmlError {}

pub(crate) fn validate_config(config: &SubscriptionConfig) -> Result<(), SubscriptionXmlError> {
    config
        .validate()
        .map_err(|_| SubscriptionXmlError::InvalidDefinition)?;
    if config.requires_session || config.max_message_bytes != MAX_MESSAGE_BYTES {
        return Err(SubscriptionXmlError::UnsupportedDefinition);
    }
    if !(5_000..=300_000).contains(&config.lock_duration_millis)
        || config.max_delivery_count > i32::MAX as u32
        || config
            .default_time_to_live_millis
            .is_some_and(|millis| !(1_000..=duration::MAX_DURATION_MILLIS).contains(&millis))
    {
        return Err(SubscriptionXmlError::InvalidDefinition);
    }
    Ok(())
}

pub(crate) fn validate_name(name: &SubscriptionName) -> Result<(), SubscriptionXmlError> {
    let value = name.as_str();
    SubscriptionName::new(value).map_err(|_| SubscriptionXmlError::UnsupportedDefinition)?;
    if value.encode_utf16().count() > 50
        || !lexical::legal_chars(value)
        || value.contains('/')
        || value.contains('\\')
        || matches!(value, "." | "..")
    {
        return Err(SubscriptionXmlError::UnsupportedDefinition);
    }
    Ok(())
}
