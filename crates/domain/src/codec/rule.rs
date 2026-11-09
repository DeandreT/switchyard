//! Canonical subscription-rule records. The value fence is local to rules: a
//! filter that predates SQL keeps its exact V1 bytes, actionless SQL uses V2,
//! and action-bearing rules use V3. Older readers refuse the new version.

use serde::{Deserialize, Serialize};

use crate::{RuleDefinition, RuleFilter, RuleName, SqlRuleFilter, Timestamp};

use super::{CodecError, VALUE_FORMAT_V1, message::CanonicalError};

pub const VALUE_FORMAT_V2: u8 = 2;
pub const VALUE_FORMAT_V3: u8 = 3;

#[derive(Serialize, Deserialize)]
struct ActionRuleV3 {
    name: String,
    filter: FilterV3,
    created_at: u64,
    action: crate::SqlRuleAction,
}

// Independent frozen filter DTOs: public filter extensions cannot silently
// become representable in a V3 action record. Postcard structs and newtypes add
// no framing, so these retain the selected V3 field layout.
#[derive(Serialize, Deserialize)]
enum FilterV3 {
    True,
    False,
    Correlation(Box<CorrelationV3>),
    Sql(String),
}

#[derive(Serialize, Deserialize)]
struct CorrelationV3 {
    correlation_id: Option<String>,
    message_id: Option<String>,
    to: Option<String>,
    reply_to: Option<String>,
    subject: Option<String>,
    session_id: Option<String>,
    reply_to_session_id: Option<String>,
    content_type: Option<String>,
    application_properties: Vec<(String, Vec<u8>)>,
}

impl FilterV3 {
    fn capture(filter: &RuleFilter) -> Self {
        match filter {
            RuleFilter::True => Self::True,
            RuleFilter::False => Self::False,
            RuleFilter::Sql(sql) => Self::Sql(sql.expression().to_owned()),
            RuleFilter::Correlation(c) => Self::Correlation(Box::new(CorrelationV3 {
                correlation_id: c.correlation_id.clone(),
                message_id: c.message_id.clone(),
                to: c.to.clone(),
                reply_to: c.reply_to.clone(),
                subject: c.subject.clone(),
                session_id: c.session_id.clone(),
                reply_to_session_id: c.reply_to_session_id.clone(),
                content_type: c.content_type.clone(),
                application_properties: c
                    .application_properties
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
                    .collect(),
            })),
        }
    }

    fn restore(self) -> RuleFilter {
        match self {
            Self::True => RuleFilter::True,
            Self::False => RuleFilter::False,
            Self::Sql(source) => RuleFilter::Sql(SqlRuleFilter::new(source)),
            Self::Correlation(c) => RuleFilter::Correlation(crate::CorrelationFilter {
                correlation_id: c.correlation_id,
                message_id: c.message_id,
                to: c.to,
                reply_to: c.reply_to,
                subject: c.subject,
                session_id: c.session_id,
                reply_to_session_id: c.reply_to_session_id,
                content_type: c.content_type,
                application_properties: c
                    .application_properties
                    .into_iter()
                    .map(|(k, v)| (k, crate::CorrelationValue::from_durable_bytes(v)))
                    .collect(),
            }),
        }
    }
}

// Stored DTO frozen independently of the public rule serde. The name is the
// display spelling, as in V1; the expression is the exact retained text.
#[derive(Serialize, Deserialize)]
struct SqlRuleV2 {
    name: String,
    expression: String,
    created_at: u64,
}

/// Rule policy, including SQL compilation, belongs to callers, so corrupt or
/// no-longer-compilable fixtures are not silently normalized here.
pub fn encode(definition: &RuleDefinition) -> Result<Vec<u8>, CodecError> {
    if let Some(action) = &definition.action {
        let mut bytes = vec![VALUE_FORMAT_V3];
        bytes.extend(
            postcard::to_stdvec(&ActionRuleV3 {
                name: definition.name.display_name().to_owned(),
                filter: FilterV3::capture(&definition.filter),
                created_at: definition.created_at.as_millis(),
                action: action.clone(),
            })
            .map_err(|_| CodecError::Encode)?,
        );
        return Ok(bytes);
    }
    match &definition.filter {
        RuleFilter::Sql(filter) => {
            let mut bytes = vec![VALUE_FORMAT_V2];
            bytes.extend(
                postcard::to_stdvec(&SqlRuleV2 {
                    name: definition.name.display_name().to_owned(),
                    expression: filter.expression().to_owned(),
                    created_at: definition.created_at.as_millis(),
                })
                .map_err(|_| CodecError::Encode)?,
            );
            Ok(bytes)
        }
        RuleFilter::True | RuleFilter::False | RuleFilter::Correlation(_) => {
            super::encode(definition)
        }
    }
}

/// Exact framing and re-encoding reject trailing, nonminimal and aliasing
/// records, including a SQL filter inside a V1 envelope.
pub fn decode(bytes: &[u8]) -> Result<RuleDefinition, CodecError> {
    decode_canonical(bytes).map_err(|error| match error {
        CanonicalError::Codec(error) => error,
        CanonicalError::NonCanonical => CodecError::Decode,
    })
}

/// [`decode`] that tells undecodable rows apart from decodable but
/// noncanonical ones, for validators that report the difference.
pub(crate) fn decode_canonical(bytes: &[u8]) -> Result<RuleDefinition, CanonicalError> {
    let (definition, unused) = read(bytes).map_err(CanonicalError::Codec)?;
    if !unused.is_empty() || encode(&definition).map_err(CanonicalError::Codec)? != bytes {
        return Err(CanonicalError::NonCanonical);
    }
    Ok(definition)
}

fn read(bytes: &[u8]) -> Result<(RuleDefinition, &[u8]), CodecError> {
    let (&version, payload) = bytes.split_first().ok_or(CodecError::EmptyEnvelope)?;
    match version {
        VALUE_FORMAT_V3 => {
            let (stored, unused) = postcard::take_from_bytes::<ActionRuleV3>(payload)
                .map_err(|_| CodecError::Decode)?;
            Ok((
                RuleDefinition {
                    name: RuleName::new(stored.name).map_err(|_| CodecError::Decode)?,
                    filter: stored.filter.restore(),
                    created_at: Timestamp::from_millis(stored.created_at),
                    action: Some(stored.action),
                },
                unused,
            ))
        }
        VALUE_FORMAT_V1 => {
            let (definition, unused) = postcard::take_from_bytes::<RuleDefinition>(payload)
                .map_err(|_| CodecError::Decode)?;
            if !definition.filter.is_legacy_representable() {
                return Err(CodecError::Decode);
            }
            Ok((definition, unused))
        }
        VALUE_FORMAT_V2 => {
            let (stored, unused) =
                postcard::take_from_bytes::<SqlRuleV2>(payload).map_err(|_| CodecError::Decode)?;
            Ok((
                RuleDefinition {
                    action: None,
                    name: RuleName::new(stored.name).map_err(|_| CodecError::Decode)?,
                    filter: RuleFilter::Sql(SqlRuleFilter::new(stored.expression)),
                    created_at: Timestamp::from_millis(stored.created_at),
                },
                unused,
            ))
        }
        version => Err(CodecError::UnsupportedVersion { version }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_v3_filters_round_trip_without_extending_legacy_rule_serde() {
        let correlation = crate::CorrelationFilter {
            subject: Some("subject".into()),
            application_properties: [(
                "a".into(),
                crate::CorrelationValue::new(vec![0x41]).unwrap(),
            )]
            .into(),
            ..crate::CorrelationFilter::default()
        };
        for filter in [
            RuleFilter::True,
            RuleFilter::False,
            RuleFilter::Correlation(correlation),
            RuleFilter::Sql(SqlRuleFilter::new("a = TRUE")),
        ] {
            let action = crate::SqlRuleAction::new("REMOVE user.old");
            let definition = RuleDefinition {
                name: RuleName::new("Exact-Name").unwrap(),
                filter: filter.clone(),
                created_at: Timestamp::from_millis(7),
                action: Some(action.clone()),
            };
            let mut expected = vec![3];
            expected.extend(postcard::to_stdvec(&("Exact-Name", filter, 7u64, action)).unwrap());
            let bytes = encode(&definition).unwrap();
            assert_eq!(bytes, expected);
            assert_eq!(decode(&bytes).unwrap(), definition);
            assert!(super::super::encode(&definition).is_err());
        }
    }

    #[test]
    fn v3_duplicate_correlation_properties_are_not_normalized_into_valid_rows() {
        let filter = RuleFilter::Correlation(crate::CorrelationFilter {
            subject: Some("subject".into()),
            ..crate::CorrelationFilter::default()
        });
        let FilterV3::Correlation(mut correlation) = FilterV3::capture(&filter) else {
            unreachable!()
        };
        correlation.application_properties =
            vec![("a".into(), vec![0x41]), ("a".into(), vec![0x42])];
        let stored = ActionRuleV3 {
            name: "rule".into(),
            filter: FilterV3::Correlation(correlation),
            created_at: 7,
            action: crate::SqlRuleAction::new("REMOVE user.old"),
        };
        let mut bytes = vec![3];
        bytes.extend(postcard::to_stdvec(&stored).unwrap());
        assert_eq!(decode(&bytes), Err(CodecError::Decode));
    }
}
