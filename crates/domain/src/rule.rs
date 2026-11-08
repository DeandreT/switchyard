use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{RuleName, SqlCompileError, SqlProgram, Timestamp};

mod action;
pub use action::{
    MAX_SQL_ACTION_PATCH_BYTES, MAX_SQL_ACTION_PATCH_PROPERTIES, MAX_SQL_ACTION_STATEMENTS,
    SqlActionCompileError, SqlActionError, SqlActionLimits, SqlActionMetrics, SqlActionPatch,
    SqlActionProgram, SqlActionProperty,
};

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
/// Most SQL rules retained across all subscriptions of one topic, Azure's
/// Standard quota. Correlation, true and false rules count only toward the
/// per-subscription limit.
pub const MAX_TOPIC_SQL_RULES: usize = 2_000;
/// Dead-letter reason Switchyard chose for a subscription copy whose SQL
/// filter failed to evaluate. It is taken from the exception name Azure's
/// documentation associates with filter errors; Azure's exact reason text is
/// not verified.
pub const FILTER_EVALUATION_DEAD_LETTER_REASON: &str = "FilterException";

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

    /// Restores already-capped proposal intent without applying current policy.
    pub(crate) fn from_durable_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
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

/// A bounded SQL predicate retained as the caller's exact expression text.
///
/// Programs are never stored: every reader compiles the text again under the
/// current grammar and limits, so a record this build cannot compile is
/// refused rather than evaluated differently.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SqlRuleFilter {
    expression: String,
}

impl SqlRuleFilter {
    /// Builds unvalidated intent; [`RuleFilter::validate`] compiles it.
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
        }
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }

    /// Compiles the expression with a fresh default budget.
    pub fn compile(&self) -> Result<SqlProgram, RuleConfigError> {
        SqlProgram::compile(&self.expression).map_err(RuleConfigError::Sql)
    }
}

/// An actionless subscription filter. Actions remain an explicit future
/// variant rather than text this build would silently accept.
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
    /// Matches only when the predicate evaluates to SQL `TRUE`. Evaluation
    /// errors are routed by topic fanout, not reported as non-matches.
    Sql(SqlRuleFilter),
}

impl RuleFilter {
    pub fn validate(&self) -> Result<(), RuleConfigError> {
        match self {
            Self::True | Self::False => Ok(()),
            Self::Correlation(filter) => filter.validate(),
            Self::Sql(filter) => filter.compile().map(drop),
        }
    }

    /// Whether the record and proposal formats that predate SQL rules can
    /// represent this filter.
    pub const fn is_legacy_representable(&self) -> bool {
        !matches!(self, Self::Sql(_))
    }

    pub(crate) fn canonicalized(&self) -> Result<Self, RuleConfigError> {
        match self {
            Self::True => Ok(Self::True),
            Self::False => Ok(Self::False),
            Self::Correlation(filter) => Ok(Self::Correlation(filter.canonicalized()?)),
            // Expression text is retained verbatim; only compilation is checked.
            Self::Sql(filter) => {
                filter.compile()?;
                Ok(Self::Sql(filter.clone()))
            }
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
    #[error("SQL filter cannot be compiled: {0}")]
    Sql(SqlCompileError),
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
    fn sql_filters_compile_on_validation_and_keep_their_text() {
        let filter = RuleFilter::Sql(SqlRuleFilter::new("Region = 'west' AND sys.Label IS NULL"));
        assert_eq!(filter.validate(), Ok(()));
        assert_eq!(filter.canonicalized(), Ok(filter.clone()));
        assert!(!filter.is_legacy_representable());
        assert!(RuleFilter::True.is_legacy_representable());
        assert!(matches!(
            RuleFilter::Sql(SqlRuleFilter::new("Region =")).validate(),
            Err(RuleConfigError::Sql(SqlCompileError::Syntax))
        ));
        assert!(matches!(
            RuleFilter::Sql(SqlRuleFilter::new("SET x = 1")).canonicalized(),
            Err(RuleConfigError::Sql(_))
        ));
    }
}
