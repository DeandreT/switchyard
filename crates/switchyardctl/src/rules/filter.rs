use std::{collections::BTreeSet, path::Path};

use admin_api::v1::{self, rule_filter::Filter};
use serde::{Deserialize, Serialize};

use super::super::{CliError, read_file};
use super::{
    MAX_FILTER_FILE_BYTES,
    scalar::{self, JsonScalar},
};

const MAX_CONDITIONS: usize = 32;
const MAX_RULE_CONTENT_BYTES: usize = 64 * 1024;
const SQL_SEMANTIC_VERSION: u32 = 1;
const MAX_SQL_SOURCE_BYTES: usize = 4096;
const MAX_SQL_SOURCE_UTF16_UNITS: usize = 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JsonProperty {
    name: String,
    value: JsonScalar,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
// A single bounded filter keeps its typed conditions together without another allocation.
#[allow(clippy::large_enum_variant)]
pub(super) enum JsonFilter {
    True {},
    False {},
    Correlation {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        to: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reply_to: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reply_to_session_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        #[serde(default)]
        properties: Vec<JsonProperty>,
    },
    Sql {
        expression: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        semantic_version: Option<u32>,
    },
}

fn invalid() -> CliError {
    CliError::Input("invalid rule filter")
}

fn validate_conditions<'a>(
    system: [Option<&str>; 8],
    mut names: impl ExactSizeIterator<Item = &'a str>,
) -> Result<(), CliError> {
    if system
        .into_iter()
        .flatten()
        .count()
        .saturating_add(names.len())
        > MAX_CONDITIONS
    {
        return Err(CliError::Input(
            "correlation filter exceeds its condition limit",
        ));
    }
    let mut unique = BTreeSet::new();
    if names.any(|name| !unique.insert(name)) {
        return Err(CliError::Input("duplicate correlation property name"));
    }
    Ok(())
}

pub(super) fn parse_json(bytes: &[u8]) -> Result<v1::RuleFilter, CliError> {
    if bytes.len() > MAX_FILTER_FILE_BYTES {
        return Err(CliError::Input(
            "rule filter JSON exceeds its file byte limit",
        ));
    }
    let filter: JsonFilter =
        serde_json::from_slice(bytes).map_err(|_| CliError::Input("invalid rule filter JSON"))?;
    let filter = match filter {
        JsonFilter::True {} => Filter::TrueFilter(v1::TrueRuleFilter {}),
        JsonFilter::False {} => Filter::FalseFilter(v1::FalseRuleFilter {}),
        JsonFilter::Correlation {
            correlation_id,
            message_id,
            to,
            reply_to,
            subject,
            session_id,
            reply_to_session_id,
            content_type,
            properties,
        } => {
            validate_conditions(
                [
                    correlation_id.as_deref(),
                    message_id.as_deref(),
                    to.as_deref(),
                    reply_to.as_deref(),
                    subject.as_deref(),
                    session_id.as_deref(),
                    reply_to_session_id.as_deref(),
                    content_type.as_deref(),
                ],
                properties.iter().map(|property| property.name.as_str()),
            )?;
            let properties = properties
                .into_iter()
                .map(|property| {
                    Ok(v1::CorrelationProperty {
                        name: property.name,
                        value: Some(scalar::into_protobuf(property.value)?),
                    })
                })
                .collect::<Result<Vec<_>, CliError>>()?;
            Filter::CorrelationFilter(v1::CorrelationRuleFilter {
                correlation_id,
                message_id,
                to,
                reply_to,
                subject,
                session_id,
                reply_to_session_id,
                content_type,
                properties,
            })
        }
        JsonFilter::Sql {
            expression,
            semantic_version,
        } => Filter::SqlFilter(v1::SqlRuleFilter {
            expression,
            semantic_version,
        }),
    };
    Ok(v1::RuleFilter {
        filter: Some(filter),
    })
}

pub(super) fn load(path: &Path) -> Result<v1::RuleFilter, CliError> {
    let bytes = read_file(
        path,
        MAX_FILTER_FILE_BYTES,
        "could not read the rule filter file",
    )
    .map_err(|_| CliError::Input("could not read a bounded regular rule filter file"))?;
    parse_json(&bytes)
}

fn scalar_content_bytes(value: &v1::RuleScalarValue) -> usize {
    use v1::rule_scalar_value::Value;
    match &value.value {
        Some(Value::BinaryValue(value)) => value.len(),
        Some(Value::StringValue(value) | Value::SymbolValue(value)) => value.len(),
        _ => 0,
    }
}

pub(super) fn from_protobuf(input: v1::RuleFilter) -> Result<JsonFilter, CliError> {
    Ok(match input.filter.ok_or_else(invalid)? {
        Filter::TrueFilter(_) => JsonFilter::True {},
        Filter::FalseFilter(_) => JsonFilter::False {},
        Filter::SqlFilter(filter) => {
            if filter.semantic_version != Some(SQL_SEMANTIC_VERSION)
                || filter.expression.len() > MAX_SQL_SOURCE_BYTES
                || filter
                    .expression
                    .encode_utf16()
                    .take(MAX_SQL_SOURCE_UTF16_UNITS + 1)
                    .count()
                    > MAX_SQL_SOURCE_UTF16_UNITS
            {
                return Err(invalid());
            }
            JsonFilter::Sql {
                expression: filter.expression,
                semantic_version: filter.semantic_version,
            }
        }
        Filter::CorrelationFilter(filter) => {
            let system = [
                filter.correlation_id.as_deref(),
                filter.message_id.as_deref(),
                filter.to.as_deref(),
                filter.reply_to.as_deref(),
                filter.subject.as_deref(),
                filter.session_id.as_deref(),
                filter.reply_to_session_id.as_deref(),
                filter.content_type.as_deref(),
            ];
            validate_conditions(
                system,
                filter
                    .properties
                    .iter()
                    .map(|property| property.name.as_str()),
            )?;
            let content_bytes = system
                .into_iter()
                .flatten()
                .fold(0_usize, |total, value| total.saturating_add(value.len()));
            let content_bytes = filter
                .properties
                .iter()
                .fold(content_bytes, |total, property| {
                    total
                        .saturating_add(property.name.len())
                        .saturating_add(property.value.as_ref().map_or(0, scalar_content_bytes))
                });
            if content_bytes > MAX_RULE_CONTENT_BYTES {
                return Err(invalid());
            }
            let properties = filter
                .properties
                .into_iter()
                .map(|property| {
                    Ok(JsonProperty {
                        name: property.name,
                        value: scalar::from_protobuf(property.value.ok_or_else(invalid)?)?,
                    })
                })
                .collect::<Result<Vec<_>, CliError>>()?;
            JsonFilter::Correlation {
                correlation_id: filter.correlation_id,
                message_id: filter.message_id,
                to: filter.to,
                reply_to: filter.reply_to,
                subject: filter.subject,
                session_id: filter.session_id,
                reply_to_session_id: filter.reply_to_session_id,
                content_type: filter.content_type,
                properties,
            }
        }
    })
}
