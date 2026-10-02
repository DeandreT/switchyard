//! Typed subscription rules and their local admission limits.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};

use crate::{BrokerError, IdentifierError, MessageValue, Timestamp};

mod action;
mod scalar;
mod sql;

pub(crate) use action::SqlActionProgram;
pub use action::{SQL_ACTION_SEMANTIC_VERSION, SqlAction};
pub use sql::{SQL_FILTER_SEMANTIC_VERSION, SqlFilter};

pub const MAX_RULE_NAME_LENGTH: usize = 50;
pub const MAX_SUBSCRIPTION_RULES: usize = 32;
pub const MAX_CORRELATION_RULE_CONDITIONS: usize = 32;
/// Exact versioned stored-value bytes, counted before allocating an encoding.
pub const MAX_RULE_BYTES: usize = 64 * 1024;
pub const MAX_SUBSCRIPTION_RULE_BYTES: usize = 256 * 1024;
pub const MAX_TOPIC_RULE_MATCH_WORK: usize = 1_048_576;
pub const MAX_TOPIC_RULE_COMPARISON_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RuleName(String);

impl RuleName {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        Self::validate(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn validate(value: &str) -> Result<(), IdentifierError> {
        let kind = "rule name";
        if value.trim().is_empty() {
            return Err(IdentifierError::Empty { kind });
        }
        if value.encode_utf16().count() > MAX_RULE_NAME_LENGTH {
            return Err(IdentifierError::TooLongUtf16 {
                kind,
                maximum: MAX_RULE_NAME_LENGTH,
            });
        }
        if value.chars().any(char::is_control) {
            return Err(IdentifierError::ControlCharacter { kind });
        }
        if value.contains(['/', '\\', '@', '?', '#', '*']) {
            return Err(IdentifierError::InvalidCharacter { kind });
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for RuleName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for RuleName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

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
    #[serde(deserialize_with = "scalar::deserialize_properties")]
    pub properties: BTreeMap<String, MessageValue>,
}

impl CorrelationFilter {
    pub fn condition_count(&self) -> usize {
        self.system_conditions()
            .into_iter()
            .flatten()
            .count()
            .saturating_add(self.properties.len())
    }

    pub fn validate(&self) -> Result<(), BrokerError> {
        if self.condition_count() > MAX_CORRELATION_RULE_CONDITIONS {
            return Err(BrokerError::InvalidRule {
                reason: format!(
                    "a correlation rule permits at most {MAX_CORRELATION_RULE_CONDITIONS} conditions"
                ),
            });
        }
        for value in self.properties.values() {
            if !is_rule_scalar(value) {
                return Err(BrokerError::InvalidRule {
                    reason: String::from("correlation property conditions require scalar values"),
                });
            }
            if matches!(value, MessageValue::Symbol(symbol) if !symbol.is_ascii()) {
                return Err(BrokerError::InvalidRule {
                    reason: String::from("correlation symbol conditions require ASCII"),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn system_conditions(&self) -> [Option<&str>; 8] {
        [
            self.correlation_id.as_deref(),
            self.message_id.as_deref(),
            self.to.as_deref(),
            self.reply_to.as_deref(),
            self.subject.as_deref(),
            self.session_id.as_deref(),
            self.reply_to_session_id.as_deref(),
            self.content_type.as_deref(),
        ]
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
// The public typed constructor avoids a separate allocation for each filter;
// the complete rule set has independent count and stored-byte ceilings.
#[allow(clippy::large_enum_variant)]
pub enum RuleFilter {
    True,
    False,
    Correlation(CorrelationFilter),
    Sql(SqlFilter),
}

impl RuleFilter {
    pub fn validate(&self) -> Result<(), BrokerError> {
        match self {
            Self::Correlation(filter) => filter.validate(),
            Self::Sql(filter) => filter
                .validate_source()
                .map_err(BrokerError::SqlRuleCompilation),
            Self::True | Self::False => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuleDefinition {
    pub name: RuleName,
    pub filter: RuleFilter,
    pub created_at: Timestamp,
    pub action: Option<SqlAction>,
}

impl RuleDefinition {
    /// Older envelopes retain their original three-field rule shape.
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::CodecError> {
        let (version, payload) = crate::codec::split(bytes)?;
        let rule = if version < crate::codec::VALUE_FORMAT_V11 {
            let (name, filter, created_at) = crate::codec::decode_payload(payload)?;
            Self {
                name,
                filter,
                created_at,
                action: None,
            }
        } else {
            crate::codec::decode_payload(payload)?
        };
        if version < crate::codec::VALUE_FORMAT_V10 && matches!(&rule.filter, RuleFilter::Sql(_)) {
            return Err(crate::CodecError::Decode);
        }
        Ok(rule)
    }

    /// Validates source semantics without re-encoding an older stored envelope.
    pub fn validate(&self) -> Result<(), BrokerError> {
        RuleName::validate(self.name.as_str())?;
        self.filter.validate()?;
        if let Some(action) = &self.action {
            action
                .validate_source()
                .map_err(BrokerError::SqlActionCompilation)?;
        }
        Ok(())
    }

    /// Validates and counts the complete stored envelope without copying it.
    pub fn encoded_size(&self) -> Result<usize, BrokerError> {
        self.validate()?;
        let size: usize =
            postcard::serialize_with_flavor(self, postcard::ser_flavors::Size::default())
                .map_err(|_| crate::CodecError::Encode)?;
        let size = size.saturating_add(1);
        if size > MAX_RULE_BYTES {
            return Err(BrokerError::RuleTooLarge {
                maximum_bytes: MAX_RULE_BYTES,
            });
        }
        Ok(size)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuleMatchLimit {
    WorkUnits,
    ComparisonBytes,
    LikePatternBytes,
    RegexEngineBytes,
}

pub(crate) fn is_rule_scalar(value: &MessageValue) -> bool {
    !matches!(
        value,
        MessageValue::List(_)
            | MessageValue::Map(_)
            | MessageValue::Array(_)
            | MessageValue::Described { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_rules_decode_without_actions_and_new_shapes_cannot_be_rolled_back()
    -> Result<(), Box<dyn std::error::Error>> {
        let rule = RuleDefinition {
            name: RuleName::new("legacy")?,
            filter: RuleFilter::True,
            created_at: Timestamp::from_millis(7),
            action: None,
        };
        let payload = postcard::to_stdvec(&(&rule.name, &rule.filter, rule.created_at))?;
        for version in crate::codec::VALUE_FORMAT_V1..=crate::codec::VALUE_FORMAT_V10 {
            let mut bytes = vec![version];
            bytes.extend_from_slice(&payload);
            assert_eq!(RuleDefinition::decode(&bytes)?, rule);
            let mut current = crate::codec::encode(&rule)?;
            current[0] = version;
            assert_eq!(
                RuleDefinition::decode(&current),
                Err(crate::CodecError::Decode)
            );
        }
        let rule = RuleDefinition {
            action: Some(SqlAction::new("REMOVE user.color;")?),
            ..rule
        };
        let bytes = crate::codec::encode(&rule)?;
        assert_eq!(RuleDefinition::decode(&bytes)?, rule);
        for version in crate::codec::VALUE_FORMAT_V1..=crate::codec::VALUE_FORMAT_V10 {
            let mut old = bytes.clone();
            old[0] = version;
            assert_eq!(RuleDefinition::decode(&old), Err(crate::CodecError::Decode));
        }
        Ok(())
    }

    #[test]
    fn names_preserve_case_spaces_and_sdk_default() -> Result<(), IdentifierError> {
        for name in ["$Default", "$Other", "priority orders", "Priority Orders"] {
            assert_eq!(RuleName::new(name)?.as_str(), name);
        }
        assert_ne!(RuleName::new("Priority")?, RuleName::new("priority")?);
        Ok(())
    }

    #[test]
    fn name_length_counts_utf16_units() {
        assert!(RuleName::new("x".repeat(50)).is_ok());
        assert!(RuleName::new("x".repeat(51)).is_err());
        assert!(RuleName::new("\u{1f600}".repeat(25)).is_ok());
        assert!(RuleName::new("\u{1f600}".repeat(26)).is_err());
    }

    #[test]
    fn name_length_diagnostic_uses_the_enforced_unit() {
        let error = RuleName::new("x".repeat(51)).expect_err("an overlong rule name");
        assert_eq!(
            error,
            IdentifierError::TooLongUtf16 {
                kind: "rule name",
                maximum: MAX_RULE_NAME_LENGTH,
            }
        );
        assert_eq!(
            error.to_string(),
            "rule name exceeds its 50-UTF-16-unit limit"
        );
    }

    #[test]
    fn invalid_names_are_rejected_by_constructor_and_decode() -> Result<(), crate::CodecError> {
        for name in [
            "",
            "   ",
            "bad/name",
            "bad\\name",
            "bad@name",
            "bad?name",
            "bad#name",
            "bad*name",
            "bad\0name",
            "bad\nname",
        ] {
            assert!(RuleName::new(name).is_err());
            assert!(crate::codec::decode::<RuleName>(&crate::codec::encode(&name)?).is_err());
        }
        Ok(())
    }

    #[test]
    fn condition_limit_counts_system_and_custom_conditions_together() {
        let mut filter = CorrelationFilter {
            subject: Some(String::from("orders")),
            ..CorrelationFilter::default()
        };
        for index in 0..31 {
            filter
                .properties
                .insert(index.to_string(), MessageValue::Null);
        }
        assert_eq!(filter.condition_count(), 32);
        assert!(filter.validate().is_ok());
        filter.message_id = Some(String::from("id"));
        assert!(matches!(
            filter.validate(),
            Err(BrokerError::InvalidRule { .. })
        ));
    }

    #[test]
    fn only_scalar_custom_values_are_admitted() {
        for value in [
            MessageValue::List(vec![]),
            MessageValue::Map(vec![]),
            MessageValue::Array(vec![]),
            MessageValue::Described {
                descriptor: crate::MessageDescriptor::Code(1),
                value: Box::new(MessageValue::Null),
            },
        ] {
            let filter = CorrelationFilter {
                properties: BTreeMap::from([(String::from("key"), value)]),
                ..CorrelationFilter::default()
            };
            assert!(matches!(
                filter.validate(),
                Err(BrokerError::InvalidRule { .. })
            ));
        }
        assert!(CorrelationFilter::default().validate().is_ok());
    }

    #[test]
    fn counted_rule_bytes_are_the_exact_stored_envelope() -> Result<(), BrokerError> {
        let rule = RuleDefinition {
            name: RuleName::new("Example")?,
            filter: RuleFilter::Correlation(CorrelationFilter {
                session_id: Some(String::from("session")),
                properties: BTreeMap::from([(
                    String::from("key"),
                    MessageValue::String(String::from("value")),
                )]),
                ..CorrelationFilter::default()
            }),
            created_at: Timestamp::UNIX_EPOCH,
            action: None,
        };
        assert_eq!(rule.encoded_size()?, crate::codec::encode(&rule)?.len());
        let oversized = RuleDefinition {
            filter: RuleFilter::Correlation(CorrelationFilter {
                subject: Some("x".repeat(MAX_RULE_BYTES)),
                ..CorrelationFilter::default()
            }),
            ..rule
        };
        assert!(matches!(
            oversized.encoded_size(),
            Err(BrokerError::RuleTooLarge { .. })
        ));
        Ok(())
    }
}
