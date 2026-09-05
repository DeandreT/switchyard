use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{RuleName, Timestamp};

/// Name of the match-all rule created atomically with every subscription.
pub const DEFAULT_RULE_NAME: &str = "$default";
/// Maximum rules retained below one subscription in this initial Standard
/// implementation. The bound keeps fanout work deterministic and finite.
pub const MAX_SUBSCRIPTION_RULES: usize = 2_000;
/// Largest page one replicated list command returns.
pub const MAX_RULE_PAGE: u32 = 100;
/// A filter definition cannot exceed the same logical size as one Standard
/// message. This bounds both replicated commands and publish-time evaluation.
pub const MAX_CORRELATION_FILTER_BYTES: usize = 256 * 1024;
pub const MAX_CORRELATION_VALUE_BYTES: usize = MAX_CORRELATION_FILTER_BYTES;

/// Canonical encoding of one scalar application-property value.
///
/// The protocol adapter owns the encoding because the domain deliberately has
/// no AMQP dependency. The encoding must include the scalar type as well as its
/// value: an AMQP `int` and `long` containing the same number are distinct
/// correlation values. Equality in the state machine is exact byte equality.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CorrelationValue(Vec<u8>);

impl CorrelationValue {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, RuleConfigError> {
        let bytes = bytes.into();
        if bytes.len() > MAX_CORRELATION_VALUE_BYTES {
            return Err(RuleConfigError::CorrelationValueTooLarge {
                bytes: bytes.len(),
                maximum: MAX_CORRELATION_VALUE_BYTES,
            });
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Broker-neutral projection of the system and application properties that a
/// correlation rule may inspect. Message bodies are intentionally absent.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct FilterProperties {
    pub correlation_id: Option<String>,
    pub message_id: Option<String>,
    pub to: Option<String>,
    pub reply_to: Option<String>,
    pub subject: Option<String>,
    pub session_id: Option<String>,
    pub reply_to_session_id: Option<String>,
    pub content_type: Option<String>,
    pub application_properties: BTreeMap<String, CorrelationValue>,
}

/// Equality predicates supported by the optimized Service Bus correlation
/// filter. Every populated member must match; string comparison is ordinal and
/// case-sensitive.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CorrelationFilter {
    pub correlation_id: Option<String>,
    pub message_id: Option<String>,
    pub to: Option<String>,
    pub reply_to: Option<String>,
    pub subject: Option<String>,
    pub session_id: Option<String>,
    pub reply_to_session_id: Option<String>,
    pub content_type: Option<String>,
    pub application_properties: BTreeMap<String, CorrelationValue>,
}

impl CorrelationFilter {
    pub fn matches(&self, properties: &FilterProperties) -> bool {
        matches_optional(&self.correlation_id, &properties.correlation_id)
            && matches_optional(&self.message_id, &properties.message_id)
            && matches_optional(&self.to, &properties.to)
            && matches_optional(&self.reply_to, &properties.reply_to)
            && matches_optional(&self.subject, &properties.subject)
            && matches_optional(&self.session_id, &properties.session_id)
            && matches_optional(&self.reply_to_session_id, &properties.reply_to_session_id)
            && matches_optional(&self.content_type, &properties.content_type)
            && self.application_properties.iter().all(|(name, expected)| {
                application_property(&properties.application_properties, name) == Some(expected)
            })
    }

    fn validate(&self) -> Result<(), RuleConfigError> {
        // Topic publications carrying a session identifier are deliberately
        // refused until subscription session fanout is implemented. Accepting
        // this predicate today would create a durable rule that no valid send
        // could ever satisfy.
        if self.session_id.is_some() {
            return Err(RuleConfigError::SessionIdNotSupported);
        }
        if self.is_empty() {
            return Err(RuleConfigError::EmptyCorrelationFilter);
        }
        canonical_properties(&self.application_properties)?;

        let system_bytes = [
            &self.correlation_id,
            &self.message_id,
            &self.to,
            &self.reply_to,
            &self.subject,
            &self.session_id,
            &self.reply_to_session_id,
            &self.content_type,
        ]
        .into_iter()
        .flatten()
        .map(String::len)
        .sum::<usize>();
        let application_bytes = self
            .application_properties
            .iter()
            .map(|(name, value)| name.len().saturating_add(value.as_bytes().len()))
            .fold(0_usize, usize::saturating_add);
        let bytes = system_bytes.saturating_add(application_bytes);
        if bytes > MAX_CORRELATION_FILTER_BYTES {
            return Err(RuleConfigError::CorrelationFilterTooLarge {
                bytes,
                maximum: MAX_CORRELATION_FILTER_BYTES,
            });
        }
        Ok(())
    }

    fn canonicalized(&self) -> Result<Self, RuleConfigError> {
        self.validate()?;
        let mut canonical = self.clone();
        canonical.application_properties = canonical_properties(&self.application_properties)?;
        Ok(canonical)
    }

    fn is_empty(&self) -> bool {
        self.correlation_id.is_none()
            && self.message_id.is_none()
            && self.to.is_none()
            && self.reply_to.is_none()
            && self.subject.is_none()
            && self.session_id.is_none()
            && self.reply_to_session_id.is_none()
            && self.content_type.is_none()
            && self.application_properties.is_empty()
    }
}

fn matches_optional(expected: &Option<String>, actual: &Option<String>) -> bool {
    expected
        .as_ref()
        .is_none_or(|expected| actual.as_ref() == Some(expected))
}

fn application_property<'a>(
    properties: &'a BTreeMap<String, CorrelationValue>,
    name: &str,
) -> Option<&'a CorrelationValue> {
    properties.get(name).or_else(|| {
        properties
            .iter()
            .find_map(|(candidate, value)| candidate.eq_ignore_ascii_case(name).then_some(value))
    })
}

fn canonical_properties(
    properties: &BTreeMap<String, CorrelationValue>,
) -> Result<BTreeMap<String, CorrelationValue>, RuleConfigError> {
    let mut canonical = BTreeMap::new();
    for (name, value) in properties {
        let mut name = name.clone();
        name.make_ascii_lowercase();
        if canonical.insert(name, value.clone()).is_some() {
            return Err(RuleConfigError::DuplicateApplicationProperty);
        }
    }
    Ok(canonical)
}

impl FilterProperties {
    pub(crate) fn canonicalized(mut self) -> Result<Self, RuleConfigError> {
        self.application_properties = canonical_properties(&self.application_properties)?;
        Ok(self)
    }
}

/// An actionless subscription filter. SQL filters and actions remain explicit
/// future variants rather than strings that this build would silently accept
/// without implementing their language.
// The correlation payload stays inline so this public durable value remains
// straightforward to construct at protocol boundaries. The number of rules
// evaluated for one subscription is bounded.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleFilter {
    True,
    False,
    Correlation(CorrelationFilter),
}

impl RuleFilter {
    pub fn validate(&self) -> Result<(), RuleConfigError> {
        match self {
            Self::True | Self::False => Ok(()),
            Self::Correlation(filter) => filter.validate(),
        }
    }

    pub fn matches(&self, properties: &FilterProperties) -> bool {
        match self {
            Self::True => true,
            Self::False => false,
            Self::Correlation(filter) => filter.matches(properties),
        }
    }

    pub(crate) fn canonicalized(&self) -> Result<Self, RuleConfigError> {
        match self {
            Self::True => Ok(Self::True),
            Self::False => Ok(Self::False),
            Self::Correlation(filter) => Ok(Self::Correlation(filter.canonicalized()?)),
        }
    }
}

/// Durable rule revision. `created_at` comes from the replicated command, not
/// from a local clock, so every replica and restart reports the same value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuleDefinition {
    pub name: RuleName,
    pub filter: RuleFilter,
    pub created_at: Timestamp,
}

impl RuleDefinition {
    pub(crate) fn default_at(created_at: Timestamp) -> Self {
        Self {
            name: RuleName::new(DEFAULT_RULE_NAME).expect("the default rule name is valid"),
            filter: RuleFilter::True,
            created_at,
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RuleConfigError {
    #[error("session-id correlation predicates require session-aware topic fanout")]
    SessionIdNotSupported,
    #[error("a correlation filter must name at least one property")]
    EmptyCorrelationFilter,
    #[error("correlation application-property names cannot differ only by ASCII case")]
    DuplicateApplicationProperty,
    #[error("correlation filter uses {bytes} bytes, exceeding the {maximum}-byte definition limit")]
    CorrelationFilterTooLarge { bytes: usize, maximum: usize },
    #[error("correlation value uses {bytes} bytes, exceeding the {maximum}-byte scalar limit")]
    CorrelationValueTooLarge { bytes: usize, maximum: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar(tag: u8, value: &str) -> CorrelationValue {
        let mut bytes = vec![tag];
        bytes.extend_from_slice(value.as_bytes());
        CorrelationValue::new(bytes).expect("a small canonical value")
    }

    #[test]
    fn correlation_matching_ands_fields_and_preserves_types_and_case() {
        let filter = CorrelationFilter {
            correlation_id: Some(String::from("Order-A")),
            message_id: Some(String::from("message-7")),
            to: Some(String::from("orders")),
            reply_to: Some(String::from("replies")),
            subject: Some(String::from("created")),
            session_id: Some(String::from("session-7")),
            reply_to_session_id: Some(String::from("reply-session-7")),
            content_type: Some(String::from("application/json")),
            application_properties: BTreeMap::from([
                (String::from("Region"), scalar(1, "west")),
                (String::from("attempt"), scalar(2, "7")),
            ]),
        };
        let matching = FilterProperties {
            correlation_id: Some(String::from("Order-A")),
            message_id: Some(String::from("message-7")),
            to: Some(String::from("orders")),
            reply_to: Some(String::from("replies")),
            subject: Some(String::from("created")),
            session_id: Some(String::from("session-7")),
            reply_to_session_id: Some(String::from("reply-session-7")),
            content_type: Some(String::from("application/json")),
            application_properties: BTreeMap::from([
                (String::from("region"), scalar(1, "west")),
                (String::from("attempt"), scalar(2, "7")),
            ]),
        };
        assert!(filter.matches(&matching));

        let mut wrong_case = matching.clone();
        wrong_case.correlation_id = Some(String::from("order-a"));
        assert!(!filter.matches(&wrong_case));

        let mut wrong_type = matching;
        wrong_type
            .application_properties
            .insert(String::from("attempt"), scalar(3, "7"));
        assert!(!filter.matches(&wrong_type));
    }

    #[test]
    fn correlation_filters_must_be_nonempty_and_bounded() {
        assert_eq!(
            RuleFilter::Correlation(CorrelationFilter {
                session_id: Some(String::from("session-7")),
                ..CorrelationFilter::default()
            })
            .validate(),
            Err(RuleConfigError::SessionIdNotSupported)
        );
        assert_eq!(
            RuleFilter::Correlation(CorrelationFilter::default()).validate(),
            Err(RuleConfigError::EmptyCorrelationFilter)
        );
        assert_eq!(
            RuleFilter::Correlation(CorrelationFilter {
                application_properties: BTreeMap::from([
                    (String::from("Priority"), scalar(1, "high")),
                    (String::from("priority"), scalar(1, "low")),
                ]),
                ..CorrelationFilter::default()
            })
            .validate(),
            Err(RuleConfigError::DuplicateApplicationProperty)
        );
        assert_eq!(
            CorrelationValue::new(vec![0; MAX_CORRELATION_VALUE_BYTES + 1]),
            Err(RuleConfigError::CorrelationValueTooLarge {
                bytes: MAX_CORRELATION_VALUE_BYTES + 1,
                maximum: MAX_CORRELATION_VALUE_BYTES,
            })
        );
    }

    #[test]
    fn actionless_rule_variants_have_expected_truth_tables() {
        let properties = FilterProperties::default();
        assert!(RuleFilter::True.matches(&properties));
        assert!(!RuleFilter::False.matches(&properties));
    }
}
