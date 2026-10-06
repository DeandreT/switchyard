use std::path::Path;

use admin_api::v1;
use serde::{Deserialize, Deserializer, Serialize};

use super::super::{CliError, read_file};
use super::MAX_ACTION_FILE_BYTES;

const MAX_SOURCE_BYTES: usize = 4096;
const MAX_SOURCE_UTF16_UNITS: usize = 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum JsonAction {
    Sql {
        expression: String,
        #[serde(
            default,
            deserialize_with = "read_version",
            skip_serializing_if = "Option::is_none"
        )]
        semantic_version: Option<u32>,
    },
}

fn read_version<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u32>, D::Error> {
    u32::deserialize(deserializer).map(Some)
}

pub(super) fn parse_json(bytes: &[u8]) -> Result<v1::SqlRuleAction, CliError> {
    if bytes.len() > MAX_ACTION_FILE_BYTES {
        return Err(CliError::Input(
            "rule action JSON exceeds its file byte limit",
        ));
    }
    let JsonAction::Sql {
        expression,
        semantic_version,
    } = serde_json::from_slice(bytes).map_err(|_| CliError::Input("invalid rule action JSON"))?;
    Ok(v1::SqlRuleAction {
        expression,
        semantic_version,
    })
}

pub(super) fn load(path: &Path) -> Result<v1::SqlRuleAction, CliError> {
    let bytes = read_file(
        path,
        MAX_ACTION_FILE_BYTES,
        "could not read the rule action file",
    )
    .map_err(|_| CliError::Input("could not read a bounded regular rule action file"))?;
    parse_json(&bytes)
}

pub(super) fn from_protobuf(input: v1::SqlRuleAction) -> Result<JsonAction, CliError> {
    if !matches!(input.semantic_version, Some(1 | 2))
        || input.expression.len() > MAX_SOURCE_BYTES
        || input
            .expression
            .encode_utf16()
            .take(MAX_SOURCE_UTF16_UNITS + 1)
            .count()
            > MAX_SOURCE_UTF16_UNITS
    {
        return Err(CliError::Input("invalid rule action response"));
    }
    Ok(JsonAction::Sql {
        expression: input.expression,
        semantic_version: input.semantic_version,
    })
}
