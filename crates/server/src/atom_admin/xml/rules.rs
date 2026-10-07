use std::fmt;

use domain::{RuleFilter, RuleName};

use super::{AtomXmlError, lexical};
use crate::AtomRuleDefinition;

mod correlation;
mod decode;
mod encode;
mod ordinal;

pub(crate) use decode::decode_definition;
pub(crate) use encode::{encode_entry, encode_error, encode_feed};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuleXmlError {
    Malformed,
    WorkLimitExceeded,
    InvalidDefinition,
    UnsupportedDefinition,
    ReplyLimitExceeded,
}

impl From<AtomXmlError> for RuleXmlError {
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

impl RuleXmlError {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::Malformed => "InvalidXml",
            Self::WorkLimitExceeded => "XmlWorkLimitExceeded",
            Self::InvalidDefinition => "InvalidRuleDefinition",
            Self::UnsupportedDefinition => "UnsupportedRuleDefinition",
            Self::ReplyLimitExceeded => "XmlReplyLimitExceeded",
        }
    }

    pub(crate) fn detail(self) -> &'static str {
        match self {
            Self::Malformed => "The rule XML document is malformed.",
            Self::WorkLimitExceeded => "The rule XML document exceeds a work limit.",
            Self::InvalidDefinition => "The rule definition is invalid.",
            Self::UnsupportedDefinition => "The rule definition is not supported.",
            Self::ReplyLimitExceeded => "The rule XML response exceeds a work limit.",
        }
    }
}

impl fmt::Display for RuleXmlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.detail())
    }
}

impl std::error::Error for RuleXmlError {}

pub(crate) fn validate_name(name: &RuleName) -> Result<(), RuleXmlError> {
    let value = name.as_str();
    RuleName::new(value).map_err(|_| RuleXmlError::UnsupportedDefinition)?;
    if !lexical::legal_chars(value) || matches!(value, "." | "..") {
        return Err(RuleXmlError::UnsupportedDefinition);
    }
    Ok(())
}

pub(crate) fn validate_definition(definition: &AtomRuleDefinition) -> Result<(), RuleXmlError> {
    validate_name(&definition.name)?;
    match &definition.filter {
        RuleFilter::True | RuleFilter::False => Ok(()),
        RuleFilter::Sql(filter) => {
            if filter.semantic_version() != domain::SQL_FILTER_SEMANTIC_VERSION
                || !lexical::legal_chars(filter.expression())
            {
                return Err(RuleXmlError::UnsupportedDefinition);
            }
            domain::SqlProgram::compile(filter.expression())
                .map(|_| ())
                .map_err(|_| RuleXmlError::InvalidDefinition)
        }
        RuleFilter::Correlation(filter) => correlation::validate(filter),
    }
}
